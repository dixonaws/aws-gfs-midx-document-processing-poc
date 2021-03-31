""" Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0. """

from elasticsearch import Elasticsearch, RequestsHttpConnection
import requests
from aws_requests_auth.aws_auth import AWSRequestsAuth
from requests_aws4auth import AWS4Auth
import base64
from s3transfer.manager import TransferManager
import os
import os.path
import sys
import boto3
import json
import time
import io
from io import BytesIO
import sys
from trp import Document

try:
    from urllib.parse import unquote_plus
except ImportError:
     from urllib import unquote_plus


print('setting up boto3')

root = os.environ["LAMBDA_TASK_ROOT"]
sys.path.insert(0, root)
print(boto3.__version__)
print('core path setup')
s3 = boto3.resource('s3')
s3client = boto3.client('s3')

host= os.environ['esDomain']
print("ES DOMAIN IS..........")
region=os.environ['AWS_REGION']

service = 'es'
credentials = boto3.Session().get_credentials()

def connectES():
 print ('Connecting to the ES Endpoint {0}')
 awsauth = AWS4Auth(credentials.access_key, 
 credentials.secret_key, 
 region, service,
 session_token=credentials.token)
 try:
  es = Elasticsearch(
   hosts=[{'host': host, 'port': 443}],
   http_auth = awsauth,
   use_ssl=True,
   verify_certs=True,
   connection_class=RequestsHttpConnection)
  return es
 except Exception as E:
  print("Unable to connect to {0}")
  print(E)
  exit(3)
print("sucess seting up es")

print("setting up Textract")
# get the results
textract = boto3.client(
         service_name='textract',
         region_name=region)

print('initializing comprehend')
comprehend = boto3.client(service_name='comprehend', region_name=region)
print('done')

def outputForm(page):
        csvData = []
        for field in page.form.fields:
            csvItem  = []
            if(field.key):
                csvItem.append(field.key.text)
            else:
                csvItem.append("")
            if(field.value):
                csvItem.append(field.value.text)
            else:
                csvItem.append("")
            csvData.append(csvItem)
        return csvData

def outputTable(page):
    csvData = []
    print("//////////////////")
    #print(page)
    for table in page.tables:
            csvRow = []
            csvRow.append("Table")
            csvData.append(csvRow)
            for row in table.rows:
                csvRow  = []
                for cell in row.cells:
                    csvRow.append(cell.text)
                csvData.append(csvRow)
            csvData.append([])
            csvData.append([])
    return csvData
# --------------- Main Lambda Handler ------------------


def handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key'])
    print("key is"+key)
    print("bucket is"+bucket)
    text=""
    text_custom=""
    textvalues=[]
    textvalues_entity={}
    textvalues_allentity={}
    textvalues_allentity_es={}
    try:
        s3.Bucket(bucket).download_file(Key=key,Filename='/tmp/{}')
        # Read document content
        with open('/tmp/{}', 'rb') as document:
            imageBytes = bytearray(document.read())
        print("Object downloaded")
        
        "EXTRACT TEXT FROM TEXTRACT"
    
        response_id = textract.start_document_analysis(DocumentLocation={
        'S3Object': {
            'Bucket': bucket,
            'Name': key
        }},FeatureTypes=["TABLES", "FORMS"])
        jobid = response_id['JobId']
        print("Job ID: "+jobid)
        job_status='IN_PROGRESS'
        while job_status=='IN_PROGRESS':
            print("no document yet waiting 3 secs")
            time.sleep(3)
            response = textract.get_document_analysis(JobId=jobid)
            print(response)
            job_status=response['JobStatus']
            
        "PARSE ALL TEXTRACT RESPONSE USING CLASSES (DOCUMENT) DEFINED IN trp.py python file"
        document = Document(response)
        table=[]
        forms=[]
        #print(document)
        for page in document.pages:
            table+=outputTable(page)
            forms+=outputForm(page)
        print(forms)
        print(table)
        blocks=response['Blocks']
        for block in blocks:
            if block['BlockType'] == 'LINE':
                text += block['Text']+"\n"
                text_custom += block['Text']+"          "
        print(text)
        
        "Extracting Key Phrases (NOT USED)"
        keyphrase_response = comprehend.detect_key_phrases(Text=text, LanguageCode='en')
        KeyPhraseList=keyphrase_response.get("KeyPhrases")
        for s in KeyPhraseList:
              textvalues.append(s.get("Text"))
        
        "Comprehend Entities from text"
        detect_entity= comprehend.detect_entities(Text=text, LanguageCode='en')
        print(detect_entity)
        EntityList=detect_entity.get("Entities")
        print(EntityList)
        entity_id=0
        for s in EntityList:
                print(s)
                if s["Score"]>0.90:
                    "Elasticsearch needs values as string not list *_es"
                    textvalues_allentity_es[str(s.get("Type").strip('\t\n\r'))+"-"+str(entity_id)]=str([s.get("Text").strip('\t\n\r'),s.get("Score")])
                    textvalues_allentity[str(s.get("Type").strip('\t\n\r'))+"-"+str(entity_id)]=[s.get("Text").strip('\t\n\r'),s.get("Score")]
                    print(textvalues_allentity)
                    entity_id+=1
                #Logic of entities selection Example Date
                textvalues_entity.update([(s.get("Type").strip('\t\n\r'),s.get("Text").strip('\t\n\r'))])
        print(textvalues_entity)
        
        
        
        "Getting Key-Value from form"
        #detect_entity= comprehend.detect_entities(Text=text, LanguageCode='en')
        forms_key_value={}
        for entitie in forms:
            forms_key_value[str(entitie[0]).strip(".,:")]=str(entitie[1]).strip(".,")
            
        
        forms_keys=forms_key_value.keys()
        print("FORM_KEY_VALUE: ",forms_key_value)
        
        
        "Comprehend Entities from forms"
        form_entities={}
        form_entities_es={}
        EntityList=[]
        entity_id=0
        for form in forms:
            for value in form:
                if value!='':
                    entities=comprehend.detect_entities(Text=value, LanguageCode='en')
                    EntityList+=entities["Entities"]
        for entitie in EntityList:
            form_entities[entitie["Type"]+str(entity_id)]=[entitie.get("Text").strip('\t\n\r'),entitie.get("Score")]
            form_entities_es[entitie["Type"]+str(entity_id)]=str([entitie.get("Text").strip('\t\n\r'),entitie.get("Score")])
            entity_id+=1
        print("FORM_ENTITIES: ",form_entities)
        
        
        
        
        """Custom Entities from Form"""
        forms_keys=forms_key_value.keys()
        form_customentities={}
        form_customentities_es={}
        EntityList=[]
        EntityListWithNewValues=[]
        entity_id=0
        for i in forms_keys:
            key_detected=comprehend.detect_entities(Text=i, LanguageCode='en',EndpointArn='arn:aws:comprehend:eu-west-1:180224691447:entity-recognizer-endpoint/custom-endpoint')
            EntityList+=key_detected["Entities"]
        print(EntityList)
        for entity in EntityList:
            try:
                form_customentities[entity["Type"]+str(entity_id)]=[forms_key_value[entity["Text"]],entity["Score"]]
                form_customentities_es[entity["Type"]+str(entity_id)]=str([forms_key_value[entity["Text"]],entity["Score"]])
                entity_id+=1
            except Exception as e:
                print(e)
        print("FORM_Custom_ENTITIES: ",form_customentities)
        
        
        """Custom Entities from Text"""
        EntityList=[]
        text_custometities={}
        text_custometities_es={}
        for block in blocks:
            if block['BlockType'] == 'LINE':
                text_custom = block['Text']+"\n"
                customs_entities_text = comprehend.detect_entities(Text=text_custom,LanguageCode='en',EndpointArn='arn:aws:comprehend:eu-west-1:180224691447:entity-recognizer-endpoint/custom-endpoint')
                EntityList+=customs_entities_text.get("Entities")
        print(EntityList)
        entity_id=0
        for entitie in EntityList:
            text_custometities[entitie["Type"]+str(entity_id)]=[entitie.get("Text").strip('\t\n\r'),entitie.get("Score")]
            text_custometities_es[entitie["Type"]+str(entity_id)]=str([entitie.get("Text").strip('\t\n\r'),entitie.get("Score")])
            entity_id+=1
        print(text_custometities)
        
        """
        SELECTION LOGIC
        
        VENDOR -> ""
        INVOICE_NUMBER -> ""
        AMOUNTTOBEPAID -> ""
        DATE -> ""
        LOCATION -> ""
        
        Extraction of requited Fields
        
        type: comprehend builtin vs custom entities or both
        mode: where do I get the data (from text or from forms)
        """
        fields=[
               {"field":"VENDOR","type":"custom","mode":"text"},
               {"field":"INVOICE_NUMBER","type":"custom","mode":"form"},
               {"field":"AMOUNTTOBEPAID","type":"custom","mode":"form"},
               {"field":"DATE","type":"builtin","mode":"all"}
               ]    
               
        print("START FINAL VALUE EXTRACTION")
        
        def get_final_value(field_name,field_type,field_mode):
            "function that get final value based on field nature"
            score_register=0
            final_entity={field_name:None}
            if field_type=="custom" and field_mode=="text":
                for entity in text_custometities:
                    if field_name in entity:
                        value=text_custometities[entity][0]
                        score=text_custometities[entity][1]
                        if score > score_register:
                            score_register=score
                            final_entity[field_name]=[value,score]
                if score_register<0.9:
                    return {field_name:None}
                else:
                    return final_entity
            elif field_type=="custom" and field_mode=="form":
                for entity in form_customentities:
                    if field_name in entity:
                        value=form_customentities[entity][0]
                        score=form_customentities[entity][1]
                        if score > score_register:
                            score_register=score
                            final_entity[field_name]=[value,score]
                if score_register<0.9:
                    return {field_name:None}
                else:
                    return final_entity
            elif field_type=="builtin" and field_mode=="all":
                temp_dict={**textvalues_allentity, **form_entities}
                for entity in temp_dict:
                    if field_name in entity:
                        value=temp_dict[entity][0]
                        score=temp_dict[entity][1]
                        if score > score_register:
                            score_register=score
                            final_entity[field_name]=[value,score]
                if score_register<0.9:
                    return {field_name:None}
                else:
                    return final_entity
            else:
                return final_entity
        
        "extracting and merging results"
        merged_results={}
        for field in fields:
            field_name=field["field"]
            field_type=field["type"]
            field_mode=field["mode"]
            merged_results = {**merged_results, **get_final_value(field_name,field_type,field_mode)}
            
        print("FINAL RESULTS: ", merged_results)
        merged_results_es = {key: str(value) for key, value in merged_results.items()}
                
        
        s3url= 'https://s3.console.aws.amazon.com/s3/object/'+bucket+'/'+key+'?region='+region
        
        #searchdata={'s3link':s3url,'KeyPhrases':textvalues,'Entity':textvalues_allentity,"key_value_pairs":forms_key_value,"Custom_Entities_text":text_custometities_es,
        #           'FinalValues':merged_results_es,'text':text, 'table':table, 'forms':forms}
        "SEND DATA TO ELASTICSEARCH"
        searchdata={'s3link':s3url,"key_value_pairs":forms_key_value,
                    'FinalValues':merged_results_es,'text':text}
        print(searchdata)
        print("connecting to ES")
        es=connectES()
        #es.index(index="resume-search", doc_type="_doc", body=searchdata)
        es.index(index="document", doc_type="_doc", body=searchdata)
        print("data uploaded to Elasticsearch")
        return 'keyphrases Successfully Uploaded'
    except Exception as e:
        print(e)
        print('Error: ')
        raise e
