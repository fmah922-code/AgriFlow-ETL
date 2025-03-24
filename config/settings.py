import os
import sys

#API Key
usda_key = os.environ['usda_key']
usda_commodity_list = ['congr_district_code', 'state_name', 'week_ending', 'county_name',
       'prodn_practice_desc', 'county_ansi', 'statisticcat_desc', 'CV (%)',
       'Value', 'country_name', 'begin_code', 'end_code', 'state_alpha',
       'year', 'domaincat_desc', 'watershed_code', 'watershed_desc',
       'sector_desc', 'country_code', 'county_code', 'region_desc',
       'util_practice_desc', 'asd_desc', 'location_desc', 'asd_code',
       'domain_desc', 'freq_desc', 'state_fips_code', 'group_desc',
       'source_desc', 'state_ansi', 'class_desc', 'short_desc', 'unit_desc',
       'load_time', 'zip_5', 'reference_period_desc', 'agg_level_desc',
       'commodity_desc']

#Mongo Variables

mongo_username = os.environ['mongo_username']
mongo_password = os.environ['mongo_password']
mongo_default_clusterName = 'USDACluster'

mongo_client = f"mongodb+srv://{mongo_username}:{mongo_password}@usdacluster.s1juy.mongodb.net/?retryWrites=true&w=majority&appName={mongo_default_clusterName}"
mongo_default_db = 'USDA'
mongo_default_colname = 'dummy_col'


mongo_default_schema = {'domaincat_desc': None, 'year': None, 'watershed_code': None,'watershed_desc': None, 'country_code': None,
 'county_code': None,'sector_desc': None, 'region_desc': None, 'begin_code': None,'end_code': None, 'state_alpha': None,
 'statisticcat_desc': None, 'county_ansi': None, 'Value': None, 'CV (%)': None, 'country_name': None, 'congr_district_code': None,
 'state_name': None, 'week_ending': None, 'county_name': None, 'prodn_practice_desc': None, 'reference_period_desc': None,
 'zip_5': None, 'load_time': None, 'agg_level_desc': None, 'commodity_desc': None, 'unit_desc': None, 'source_desc': None,
 'class_desc': None, 'state_ansi': None, 'short_desc': None, 'asd_code': None, 'location_desc': None, 'util_practice_desc': None,
 'asd_desc': None, 'state_fips_code': None, 'freq_desc': None,  'domain_desc': None, 'group_desc': None}

#Excluded Commodities 

excluded_commodities = ['CONSUMERPRICEINDEX','COLDSTORAGECAPACITY','NON-CITRUSOTHER',
'CITRUSOTHER','VEGETABLESOTHER','PROPAGATIVEMATERIAL','DAIRYPRODUCTSOTHER','FIELDCROPSOTHER',
'ANIMALSOTHER', 'MACHINERYOTHER', 'PRODUCTIONITEMSCONSUMERPRICEINDEX', 'BUILDINGMATERIALS',
'EXPENSESOTHER', 'PRICEINDEXRATIO', 'GRASSESLEGUMESOTHER', 'FOODCROPOTHER', 'FRUITOTHER',
'POULTRYOTHER','PPITW','CASHRECEIPTTOTALS','GOVTPROGRAMTOTALS','VEGETABLETOTALS','COLDSTORAGECAPACITY',
'BEDDINGPLANTTOTALS', 'FIELDCROPTOTALS', 'CITRUSTOTALS', 'POULTRYTOTALS', 'FOODCROPTOTALS',
'HORTICULTURETOTALS', 'MACHINERYTOTALS', 'NURSERYTOTALS', 'GRAINSTORAGECAPACITY', 'EXPENSETOTALS',
'FRUITTOTALS', 'BERRYTOTALS', 'TREENUTTOTALS', 'GRASSESLEGUMESTOTALS', 'FLORICULTURETOTALS', 'CHEMICALTOTALS',
'DAIRYPRODUCTTOTALS', 'AQUACULTURETOTALS', 'CROPTOTALS', 'FERTILIZERTOTALS', 'LIVESTOCKTOTALS',
'COMMODITYTOTALS', 'SPECIALTYANIMALTOTALS', 'SHEEPGOATSTOTALS', 'NON-CITRUSTOTALS','FEEDPRICERATIO', 'FARMOPERATIONS', 'PRICEINDEXRATIO',
'PRODUCERSPRIMARY','TRUCKSAUTOS','POULTRYBY-PRODUCTMEALS','PRACTICES','FOODCOMMODITIES','PRODUCTIONITEMS',
'PLUM-APRICOTHYBRIDS']