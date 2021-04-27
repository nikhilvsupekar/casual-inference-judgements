# Author: Sandeep
from bs4 import BeautifulSoup
import lxml
import os
import datetime as dt

import json
import zipfile

class fileMetadataExtractor():
    def __init__(self, fileContent):
        self.fileMetadata = {}
        self.entries = []
        self.fileContent = fileContent
        self.tag_changes = {
            'lnpub:publishType':'lnpub:publishtype',
            'lnpub:feedMeta': 'lnpub:feedmeta',
            'icceProcessId': 'icceprocessid',
            'lnpub:entryMeta':'lnpub:entrymeta',
            'bundleId':'bundleid',
            'icceId':'icceid',
            'lnpub:incrementalType':'lnpub:incrementaltype',
            'deliveryBundleId':'deliverybundleid',
            'deliveryGroupId':'deliverygroupid',
        }
        
    def modify_input(self):
        for tag in self.tag_changes:
            self.fileContent = self.fileContent.replace(tag, self.tag_changes[tag])
        self.content = BeautifulSoup(self.fileContent, 'lxml')
        
        
    def null_check(self, item, attribute):
        if attribute =='text':            
            try:
                return item.text
            except AttributeError:
                return None
        else:            
            try:
                return item[attribute]
            except AttributeError:
                return None
            except KeyError:
                return None
            except TypeError:
                return None
            

            
    def get_metadata(self):
        self.title = self.null_check(self.content.find('title'), 'text')
        self.sub_title = self.null_check(self.content.find('subtitle'), 'text')
        self.meta_id = self.null_check(self.content.find('id'), 'text')
        self.updated = self.null_check(self.content.find('updated'), 'text')
        self.publish_type = self.null_check(self.content.find('lnpub:publishtype'), 'text')
        self.incremental_type = self.null_check(self.content.find('lnpub:incrementaltype'), 'text')
        self.info = self.content.find('lnpub:feedmeta')
        self.info_text = self.null_check(self.content.find('lnpub:feedmeta'), 'text')        
        self.deliveryBundleId = self.null_check(self.info, 'deliverybundleid')
        self.deliveryGroupId = self.null_check(self.info, 'deliverygroupid')
        self.icceProcessId = self.null_check(self.info, 'icceprocessid')

        for entry in self.content.find_all('entry'):
            entry_title = self.null_check(entry.find('title'), 'text')
            entry_id = self.null_check(entry.find('id'), 'text')
            entry_updated = self.null_check(entry.find('updated'), 'text')
            entry_meta = entry.find('lnpub:entrymeta')
            entry_meta_text = self.null_check(entry_meta, 'text')
            entry_bundle_id = self.null_check(entry_meta, 'bundleid')
            entry_icceId = self.null_check(entry_meta, 'icceid')
            entry_pcsi = self.null_check(entry_meta, 'pcsi')
            entry_src = self.null_check(entry.content, 'src')
            self.entries.append(
                {                    
                    'file_entry_title' : entry_title,
                    'file_entry_id' : entry_id,
                    'file_entry_updated' : entry_updated,
                    'file_entry_bundle_id' : entry_bundle_id,
                    'file_entry_icceid' : entry_icceId,
                    'file_entry_pcsi' : entry_pcsi,
                    'file_entry_src' : entry_src,
                })

        self.fileMetadata['file_title'] = self.title
        self.fileMetadata['file_subtitle'] = self.sub_title
        self.fileMetadata['file_meta_id'] = self.meta_id
        self.fileMetadata['file_updated'] = self.updated
        self.fileMetadata['file_publish_type'] = self.publish_type
        self.fileMetadata['file_incremental_type'] = self.incremental_type
        self.fileMetadata['file_delivery_bundle_id'] = self.deliveryBundleId
        self.fileMetadata['file_delivery_group_id'] = self.deliveryGroupId
        self.fileMetadata['entries'] = self.entries
        return self.fileMetadata

        
        
class caseMetadataExtractor():
    
    def __init__(self, caseStr):
        self.caseStr = caseStr
        self.related_data = {}
        self.metadata = {}
        self.tag_changes = {
            'keyValue': 'keyvalue',  
            'normalizedShortName':'normalizedlongname',
            'normalizedshortname':'normalizedshortname',
            'dateText': 'datetext',
            'publicationStatus': 'publicationstatus',
            'className': 'classname',
            'caseInfo': 'caseinfo',
            'fullCaseName': 'fullcasename',
            'historyCite': 'historycite',
            'decisionDates': 'decisiondates',
            'courtCaseDocHead': 'courtcasedochead',
            'courtName': 'courtname',
            'caseName': 'casename',
            'docketNumber': 'docketnumber',
            'appealHistory': 'appealhistory',
            'courtCaseDocBody': 'courtcasedocbody',
            'caseOpinions': 'caseopinions',
            'classificationItem': 'classificationitem',
            'classCode': 'classcode',
            'courtInfo': 'courtinfo',
            'courtCaseDoc': 'courtcasedoc',
            'shortCaseName': 'shortcasename',
            'locatorKey': 'locatorkey',
            'caseHistory': 'casehistory',
            'keyName': 'keyname',
            'filedDate': 'fileddate',
            'caseOpinionBy': 'caseopinionby',
            'jurisSystem': 'jurissystem',
            'classificationGroup': 'classificationgroup',
            'decisionDate': 'decisiondate',
            'bodyText': 'bodytext',
            'paginationScheme': 'paginationscheme',
            'citeForThisResource': 'citeforthisresource',
            'nameText': 'nametext',
            'citeDefinition':'citedefinition',
            'pageScheme':'pagescheme',
            'normalizedCite':'normalizedcite',
            'classificationScheme':'classificationscheme',
            'sourceScheme':'sourcescheme',
            'identifierScheme':'identifierscheme'
        }               
        self.all_citations = []

        
    def modify_input(self):
        for tag in self.tag_changes:
            self.caseStr = self.caseStr.replace(tag, self.tag_changes[tag])
        self.caseStr = BeautifulSoup(self.caseStr, 'lxml')
        self.courtCaseDocHead = self.caseStr.find('courtcasedochead')
        self.courtCaseDocBody = self.caseStr.find('courtcasedocbody')           
        
    def null_check(self, item, attribute):
        if attribute=='text':
            try:
                return item.text
            except:
                return None
        else:
            if item is None:
                return None

            try:
                return item[attribute]
            except AttributeError:
                return None
            except KeyError:
                return None

        
    def caseName(self):
        try:
            self.fullCaseName = self.null_check(self.courtCaseDocHead.find('casename').find('fullcasename'), 'text')
        except:
            self.fullCaseName = None
        try:
            self.shortCaseName = self.null_check(self.courtCaseDocHead.find('casename').find('shortcasename'), 'text')
        except:
            self.shortCaseName = None
        try:
            self.party1 = self.null_check(self.courtCaseDocHead.find('casename').find('shortcasename'), 'party1')
        except:
            self.party1 = None
        try:
            self.party2 = self.null_check(self.courtCaseDocHead.find('casename').find('shortcasename'), 'party2')        
        except:
            self.party2 = None
            
        return self.fullCaseName, self.shortCaseName, self.party1, self.party2
    
    def docketNumber(self):
        return self.null_check(self.courtCaseDocHead.find('docketnumber'), 'text')
    
    def courtInfo(self):
        info = self.courtCaseDocHead.find('courtinfo')
        try:
            self.courtName = self.null_check(info.find('courtname'), 'text')
        except:
            self.courtName = None
        #self.jurisdiction = self.null_check(info.find('jurisdiction'), 'text')
        #self.normalizedLongName = self.null_check(self.jurisdiction, 'normalizedlongname')
        #self.normalizedShortName = self.null_check(self.jurisdiction, 'normalizedshortname')
        return self.courtName #self.normalizedLongName, self.normalizedShortName
    
    def citeForThisResource(self):
        info = self.courtCaseDocHead.find('citeforthisresource')
        self.thisCaseCite = self.null_check(info, 'text')
        self.thisCiteDefinition = self.null_check(info,'citedefinition')
        self.thisCitePageScheme = self.null_check(info,'pagescheme')
        return self.thisCaseCite, self.thisCiteDefinition, self.thisCitePageScheme
    
    def decisionDate(self):
        info = self.courtCaseDocHead.find('decisiondate')
        if info:
            self.decisionDate = self.null_check(info, 'text')
            self.decisionDateDay = self.null_check(info, 'day')
            self.decisionDateMonth = self.null_check(info,'month')
            self.decisionDateYear = self.null_check(info,'year')
        else:
            self.decisionDate = None
            self.decisionDateDay = None
            self.decisionDateMonth = None
            self.decisionDateYear = None
        
        return self.decisionDate, self.decisionDateDay, self.decisionDateMonth, self.decisionDateYear
    
    def filingDate(self):
        info = self.courtCaseDocHead.find('fileddate')
        if info:
            self.filedDate = self.null_check(info, 'text')
            self.filedDateDay = self.null_check(info, 'day')
            self.filedDateMonth = self.null_check(info, 'month')
            self.filedDateYear = self.null_check(info,'year')
        else:
            self.filedDate = None
            self.filedDateDay = None
            self.filedDateMonth = None
            self.filedDateYear = None
        
        return self.filedDate, self.filedDateDay, self.filedDateMonth, self.filedDateYear
    
    def panel(self):
        try:
            self.judge = self.null_check(self.courtCaseDocBody.find('panel'), 'text')
        except:
            self.judge = None
            
        return self.judge
    
    def case_opinion_by(self):
        try:
            self.caseOpinionBy = self.null_check(self.courtCaseDocBody.find('caseopinionby'), 'text')
        except:
            self.caseOpinionBy = None
            
        return self.caseOpinionBy
    

        
    def get_citations(self):
        info = self.courtCaseDocBody.find_all('citation') 
        for i in range(len(info)):

            citation = info[i]
            cite_name = self.null_check(citation.find('emphasis'), 'text')
            #cite_text = str(citation).split('<content')[0].split('>')[-1]

            try:
                cite_norm_name = citation['normalizedcite']
            except:
                cite_norm_name = None
            try:
                cite_type = citation['type']
            except:
                cite_type = None
            try:
                guid = citation['guid']
            except:
                guid = None
            try:
                cite_id = citation['id']
            except:
                cite_id = None
                
            trailing_text = str(citation).split('</content')[0].split('>')[-1].strip()
                
            citation_data = {
                'cite_name':cite_name,
                #'cite_text':cite_text,
                #'cite_norm_name':cite_norm_name,
                'cite_type':cite_type,
                #'guid': guid,
                'cite_id': cite_id,
                #'trailing_text':trailing_text
            }

            spans = []
            for c in citation.find_all('content'):                
                for span in c.find_all('span'):
                    span_info = {}
                    
                    span_info['normalized_cite'] = span['normalizedcite']
                    #span_info['normalized_cite_text'] = str(span).split('<locator>')[0].split('>')[-1]
                    span_info['locator_keyname'] = span.find('keyname')['name']
                    span_info['locator_keyvalue'] = span.find('keyvalue')['value']

                    spans.append(span_info)
            
            
            citation_data['spans'] = spans
            self.all_citations.append(citation_data)
        return self.all_citations
    
    def get_related_data(self):
        
        self.mdata = self.caseStr.find('metadata')
        
        self.dc_meta = self.mdata.find('dc:metadata')
        self.dc_creator = self.null_check(self.dc_meta.find('dc:creator'), 'text')
        self.dc_publisher = self.null_check(self.dc_meta.find('dc:publisher'), 'text')
        self.dc_identifier_scheme = self.null_check(self.dc_meta.find('dc:identifier'), 'identifierscheme')
        self.dc_identifier = self.null_check(self.dc_meta.find('dc:identifier'), 'text')
        self.dc_sourcescheme = self.null_check(self.dc_meta.find('dc:source'), 'sourcescheme')
        self.dc_source = self.null_check(self.dc_meta.find('dc:source'), 'text')
        self.dc_date = self.null_check(self.dc_meta.find('dc:date'), 'text')
        self.paginationscheme = self.null_check(self.mdata.find('paginationscheme'), 'pagescheme')
        self.related_data['dc_creator'] = self.dc_creator
        self.related_data['dc_publisher'] = self.dc_publisher
        self.related_data['dc_identifier'] = self.dc_identifier
        self.related_data['dc_identifier_scheme'] = self.dc_identifier_scheme
        self.related_data['dc_sourcescheme'] = self.dc_sourcescheme
        self.related_data['dc_source'] = self.dc_source
        self.related_data['dc_date'] = self.dc_date
        self.related_data['paginationscheme'] = self.paginationscheme
        
        classes = []
        for group in self.mdata.find_all('classificationgroup'):
            classificationscheme = self.null_check(group, 'classificationscheme')
            classification = group.find_all('classification')
            for class_ in classification:
                classificationscheme_finer = self.null_check(class_, 'classificationscheme')
                classification_items = class_.find_all('classificationitem')
                for classification_item in classification_items:
                    try:
                        classification_item['score']

                    except KeyError:   
                        self.null_check(classification_item.find('classname'), 'text')
                        self.null_check(classification_item.find('classcode'), 'text')                

                        for sub_classification_item in classification_item.find_all('classificationitem'):
                            score = self.null_check(sub_classification_item,'score')
                            class_name = self.null_check(sub_classification_item.find('classname'), 'text')
                            class_code = self.null_check(sub_classification_item.find('classcode'), 'text')
                            classes.append((classificationscheme, classificationscheme_finer, score, class_name, class_code))

        self.related_data['classes'] = classes
        return self.related_data
        
    
    def get_all_info(self):
        self.caseName()
        self.case_opinion_by()
        self.courtInfo()
        self.citeForThisResource()
        self.decisionDate()
        self.filingDate()
        self.citeForThisResource()
        self.panel()
        self.get_citations()
        self.get_related_data()       
        
        self.metadata['docket_number'] = self.docketNumber()
        self.metadata['fullCaseName'] = self.fullCaseName
        self.metadata['shortCaseName'] = self.shortCaseName
        self.metadata['party1'] = self.party1
        self.metadata['party2'] = self.party2
        self.metadata['courtName'] = self.courtName
        #self.metadata['normalizedLongName'] = self.normalizedLongName
        #self.metadata['normalizedShortName'] = self.normalizedShortName
        self.metadata['thisCaseCite'] = self.thisCaseCite
        self.metadata['thisciteDefinition'] = self.thisCiteDefinition
        self.metadata['thisCitePageScheme'] = self.thisCitePageScheme
        self.metadata['caseOpinionBy'] = self.caseOpinionBy
        self.metadata['decisionDate'] = self.decisionDate
        self.metadata['decisionDateDay'] = self.decisionDateDay
        self.metadata['decisionDateMonth'] = self.decisionDateMonth
        self.metadata['decisionDateYear'] = self.decisionDateYear
        self.metadata['filedDate'] = self.filedDate
        self.metadata['filedDateDay'] = self.filedDateDay
        self.metadata['filedDateMonth'] = self.filedDateMonth
        self.metadata['filedDateYear'] = self.filedDateYear
        self.metadata['judges'] = self.judge
        self.metadata['citingCases'] = self.all_citations
        self.metadata['related_data'] = self.related_data
        
        return self.metadata

import re

def padding_function(x,y):
    if not (x and y):
        return None
    if len(y)<5:
        return x+'0'*(5-len(y))+y
    elif len(y)==5:
        return x+y
    else:
        return None

def standardize_docket_num(s):
    if s is None:
        return None
    
    nums = re.findall('[0-9]+', s)
    if len(nums)==2:
        return padding_function(*nums)
    elif len(nums) > 2:
        ret_list = []
        for i in range(len(nums)//2):
            
            ret_list.append(padding_function(*nums[2*i:2*i+2]))
            if len(nums) % 2 == 1:
                ret_list.append(nums[-1])
            
        return '|'.join(ret_list)
    elif len(nums) == 1:
        return nums[0]
    else:
        return None


# parse data from file 
def parse_file(fpath, content=None, zipped_file=True, outfile=None):
    """ read file and extract as lists to input into the database

    Args:
        fname (str): [path to the file in the data folder (or to zip file with appropriate modification)]

    Returns:
        d (dict): 
    """
    data = []
    file_name = os.path.basename(os.path.normpath(fpath))

    if zipped_file:
        sep='\r\n\r\n'
    else:
        with open(fpath, 'r') as f:
            content = f.read()
            sep = '\n\n'
    
    c = content.split(sep)
    
    if len(c)>3:

        file_ = fileMetadataExtractor(c[1])
        file_.modify_input()
        file_metadata = file_.get_metadata()
        
        file_title = file_metadata.get('file_title', None)
        file_subtitle = file_metadata.get('file_subtitle', None)
        file_meta_id = file_metadata.get('file_meta_id', None)
        file_updated = file_metadata.get('file_updated', None)
        file_publish_type = file_metadata.get('file_publish_type', None)
        file_incremental_type = file_metadata.get('file_incremental_type', None)
        file_delivery_bundle_id = file_metadata.get('file_delivery_bundle_id', None)
        file_delivery_group_id = file_metadata.get('file_delivery_group_id', None)
        
        file_meta = {
            'file_title': file_title,
            'file_subtitle': file_subtitle,
            'file_name': file_name,
            #'file_meta_id': file_meta_id,
            'file_updated': file_updated,
            'file_publish_type' : file_publish_type,
            'file_incremental_type' : file_incremental_type,
            'file_delivery_bundle_id': file_delivery_bundle_id,
            'file_delivery_group_id': file_delivery_group_id
        }
        
        entries = []
        for entry in file_metadata['entries']: 
            file_entry_title = entry.get('file_entry_title', None)
            file_entry_id = entry.get('file_entry_id', None)
            file_entry_updated = entry.get('file_entry_updated', None)
            file_entry_bundle_id = entry.get('file_entry_bundle_id', None)
            file_entry_icceid = entry.get('file_entry_icceid', None)
            file_entry_pcsi = entry.get('file_entry_pcsi', None)
            file_entry_src = entry.get('file_entry_src', None)
            
            entries.append({
                'file_entry_title':file_entry_title,
                'file_entry_id': file_entry_id,
                'file_entry_updated': file_entry_updated,
                'file_entry_bundle_id': file_entry_bundle_id,
                'file_entry_icceid': file_entry_icceid,
                'file_entry_pcsi': file_entry_pcsi,
                'file_entry_src': file_entry_src,                
            })
        try:
            n_iter = iter(range(len(entries)))
        except:
            n_iter = None        
        
        for item in c[3::2]:
            if '<?xml' not in item:
                continue

            case = caseMetadataExtractor(item)
            case.modify_input()
            case_meta = case.get_all_info()
            case_text = str(case.courtCaseDocBody)
            #cases_meta.append(case_meta)
            
            # pick the correct entry for the entries table
            if n_iter:
                n = next(n_iter)
                entry = entries[n]
            else:
                entry = {
                'file_entry_title':None,
                'file_entry_id': None,
                'file_entry_updated': None,
                'file_entry_bundle_id': None,
                'file_entry_icceid': None,
                'file_entry_pcsi': None,
                'file_entry_src': None,                
            }
                
            # prepare case data
            docket_num = case_meta.get('docket_number')
            docket_num_normalized = case_meta.get('normalized docket_num')
            full_case_name = case_meta.get('fullCaseName')
            short_case_name = case_meta.get('shortCaseName')
            party_1 = case_meta.get('party1')
            party_2 = case_meta.get('party2')
            court_name = case_meta.get('courtName')
            court_code = case_meta.get('court_code') # court_code
            this_case_cite = case_meta.get('thisCaseCite')
            this_cite_definition = case_meta.get('thisciteDefinition')
            this_cite_page_scheme = case_meta.get('thisCitePageScheme')
            case_opinion_by = case_meta.get('caseOpinionBy')
            judges = case_meta.get('judges')
            decision_date_str = case_meta.get('decisionDate')
            decision_date_day = case_meta.get('decisionDateDay')
            decision_date_month = case_meta.get('decisionDateMonth')
            decision_date_year = case_meta.get('decisionDateYear')
            try:
                d_day = int(decision_date_day)
                d_month = int(decision_date_month)
                d_year = int(decision_date_year)
                decision_date = dt.date(d_year, d_month, d_day)
            except:
                decision_date = None
            filing_date_str = case_meta.get('filedDate')
            filing_date_day = case_meta.get('filedDateDay')
            filing_date_month = case_meta.get('filedDateMonth')
            filing_date_year = case_meta.get('filedDateYear')
            try:
                f_day = int(filing_date_day)
                f_month = int(filing_date_month)
                f_year = int(filing_date_year)
                filing_date = dt.date(f_year, f_month, f_day)
            except:
                filing_date = None
            dc_creator = case_meta['related_data']['dc_creator']
            dc_identifier = case_meta['related_data']['dc_identifier']
            dc_identifier_scheme = case_meta['related_data']['dc_identifier_scheme']
            dc_source_scheme = case_meta['related_data']['dc_sourcescheme']
            dc_source = case_meta['related_data']['dc_source']
            dc_date = case_meta['related_data']['dc_date']
            pagination_scheme = case_meta['related_data']['paginationscheme']
            
            case_data = {                
                'file_entry_bundle_id':entry['file_entry_bundle_id'],
                'docket_num':docket_num,
                'docket_num_normalized':docket_num_normalized,
                'full_case_name':full_case_name,
                'short_case_name':short_case_name,
                'party_1':party_1,
                'party_2':party_2,
                'court_name':court_name,
                'court_code':court_code,
                'this_case_cite':this_case_cite,
                'this_cite_definition':this_cite_definition,
                'this_cite_page_scheme':this_cite_page_scheme,
                'case_opinion_by':case_opinion_by,
                'judges':judges,
                'decision_date_str':decision_date_str,
                'decision_date':decision_date,
                'filing_date_str':filing_date_str,
                'filing_date':filing_date,
                'dc_creator':dc_creator,
                'dc_identifier':dc_identifier,
                'dc_identifier_scheme':dc_identifier_scheme,
                'dc_source':dc_source,
                'dc_date':dc_date,
                'pagination_scheme':pagination_scheme
            }
            
            # note cited_docs and doc_details will be a list of dicts 
            cited_docs, doc_details = [], []
            for doc in case_meta['citingCases']:

                #case_id = this_case_cite
                cite_id = doc.get('cite_id', None)
                cite_type = doc.get('cite_type', None)
                cite_name = doc.get('cite_name', None)
                cited_docs.append({
                    #'case_id':case_id,
                    'cite_id':cite_id,'data/fffa6651-78a9-4021-aed4-dacfad713725'
                    'cite_type':cite_type,
                    'cite_name':cite_name
                })
                
                # update doc_details
                spans = doc['spans']
                doc_details_set = set([])
                if spans:
                    for span in spans:
                        normalized_cite = span.get('normalized_cite', None)
                        locator_keyname = span.get('locator_keyname', None)
                        locator_keyvalue = span.get('locator_keyvalue', None)
                        doc_details_set.add((
                            cite_id,
                            cite_type,
                            normalized_cite,
                            locator_keyname,
                            locator_keyvalue
                        ))
                for item in doc_details_set:
                    doc_details.append((item[0], dict(zip(('cite_type', 'normalized_cite', 'locator_keyname', 'locator_keyvalue'), item[1:]))))
                        
                        
            # extract class items
            classes = []
            class_items_dict = case_meta.get('related_data')
            class_items = class_items_dict.get('classes')

            if class_items:
                for class_ in class_items:
                    classes.append({

                        'classification_scheme':class_[0],
                        'classification_scheme_fine':class_[1],
                        'score':class_[2],
                        'class_name':class_[3],
                        'class_code':class_[4]
                    })       
            
            d = {
                'file_meta' : file_meta,
                'case_meta' : case_data,
                'file_entry': entry,
                'cited_docs': cited_docs,
                'doc_details': doc_details,
                'classes' : classes,
                'case_text': {'case_text':case_text}
            }
            
            flattened_d = {
                'file_entry_bundle_id' : d['case_meta']['file_entry_bundle_id'],
                'docket_num' : d['case_meta']['docket_num'],
                'docket_num_standardized' : standardize_docket_num(d['case_meta']['docket_num']),
                'docket_num_normalized' : d['case_meta']['docket_num_normalized'],
                'full_case_name' : d['case_meta']['full_case_name'],
                'short_case_name' : d['case_meta']['short_case_name'],
                'party_1' : d['case_meta']['party_1'],
                'party_2' : d['case_meta']['party_2'],
                'court_name' : d['case_meta']['court_name'],
                'court_code' : d['case_meta']['court_code'],
                'this_case_cite' : d['case_meta']['this_case_cite'],
                'this_cite_definition' : d['case_meta']['this_cite_definition'],
                'this_cite_page_scheme' : d['case_meta']['this_cite_page_scheme'],
                'case_opinion_by' : d['case_meta']['case_opinion_by'],
                'judges' : d['case_meta']['judges'],
                'decision_date_str' : d['case_meta']['decision_date_str'],
                'decision_date' : d['case_meta']['decision_date'],
                'filing_date_str' : d['case_meta']['filing_date_str'],
                'filing_date' : d['case_meta']['filing_date'],
                'dc_creator' : d['case_meta']['dc_creator'],
                'dc_identifier' : d['case_meta']['dc_identifier'],
                'dc_identifier_scheme' : d['case_meta']['dc_identifier_scheme'],
                'dc_source' : d['case_meta']['dc_source'],
                'dc_date' : d['case_meta']['dc_date'],
                'pagination_scheme' : d['case_meta']['pagination_scheme'],
                'cited_docs': cited_docs,
                'doc_details': doc_details,
                'classes' : classes
            }
            
            data.append(flattened_d)
    
    if outfile is not None:
        with open(outfile, 'a') as fout:
            for d in data:
                json.dump(d, fout, default = str)
                fout.write('\n')

    return data


def parse_zip(zip_path, outfile):

    if os.path.exists(outfile):
        os.remove(outfile)

    zf = zipfile.ZipFile(zip_path, 'r')
    xml_list = zf.namelist()

    for path in xml_list:
        if path.endswith('/'):
            continue

        print('parsing ' + path)
        
        f = zf.open(path)
        xml_content = f.read().decode('utf-8')
        parse_file(zip_path, content = xml_content, zipped_file = True, outfile = outfile)



if __name__ == "__main__":

    parse_from_zip = True
    outfile = 'data/data.json'

    if not parse_from_zip:
        fpaths = [os.path.join('data/raw_data/', x) for x in os.listdir('data/raw_data/')]
        for path in fpaths[:1]:
            print(parse_file(path, zipped_file=False))
    
    else:
        if os.path.exists(outfile):
            os.remove(outfile)

        zip_path = 'data/opinions/6411.zip'
        parse_zip(zip_path, outfile)

