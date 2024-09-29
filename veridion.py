
#facebook <=Join by name <-> legal name =>  google dataset
#facebook <=JOIN by domain <-> <rootdomain=> website_dataset.csv #
import scrubadub
from googletrans import Translator
import pandas as pd 
import numpy as np
import csv 

translator = Translator()

# text = """
# My email is jane.doe123@gmail.com and my phone number is 555-987-6543.
# You can meet us on avenue new york 34th street.
# """
# # Curățarea textului
# cleaned_text = scrubadub.clean(text)
# print(cleaned_text)
#  scrubadub doesnt seem to work well

def zip_validator(s):
    if isinstance(s, str):  
        return any(char.isdigit() for char in s)
    return False  

def name_validator(s):
    if isinstance(s, str): 
        return any(char.isdigit() for char in s)
    return False 

def email_validator(email):
    if isinstance(email, str): 
        return email.count('@') == 1 
    return False  

def phone_validator(no):
    if isinstance(no, str):  
        return any(char.isalpha() for char in no)
    return False  

def link_validator(link):
    if isinstance(link, str): 
        return 'http' in link  
    return False  

def validate_columns(df):
    for col in df.columns:
        if col == 'zip_code':
            df[col] = df[col].apply(lambda x: x if zip_validator(x) else '')
        elif col == 'email':
            df[col] = df[col].apply(lambda x: x if email_validator(x) else '')
        elif col == 'phone' or col == "raw_phone":
            df[col] = df[col].apply(lambda x: x if phone_validator(x) else '')
        elif col == 'link' :
            df[col] = df[col].apply(lambda x: x if link_validator(x) else '')


def translate_text(text):
    print(text)
    try:
        translated = translator.translate(text, dest='en').text
        
        return translated
    except:
        return text

pd.options.display.float_format = '{:.0f}'.format


cols_to_use_facebook = list(range(16))
cols_to_use_google = list(range(15))
# to avoid errors and skips (using: on_bad_lines='skip') like this one : Skipping line 66578: expected 16 fields, saw 20
facebook = pd.read_csv(r'facebook_dataset.csv',  usecols=cols_to_use_facebook  ,  encoding='utf-8', on_bad_lines='warn')
google = pd.read_csv(r'google_dataset.csv',usecols = cols_to_use_google ,on_bad_lines='warn' )   #
website = pd.read_csv(r'website_dataset.csv',on_bad_lines='warn',sep=';', header=0 ) #on_bad_lines='skip'

# dropping white lines
facebook.dropna(how='all', inplace=True)
google.dropna(how='all', inplace=True)
website.dropna(how='all', inplace=True)


print(website)
# website.to_csv('website2.csv', index=False) just to have it locally for better understanding 

# cleaning  & verifying data  : zip codes from addresses seemed ok
validate_columns(facebook)
validate_columns(google)
validate_columns(website)


# translating the cols - it takes a while 
# cols_to_translate_facebook = ['domain','address','categories','city','country_name','description','email','link','name','page_type']
# for col in cols_to_translate_facebook:
#     print(col)
#     facebook[col] = facebook[col].apply(lambda x: translate_text(str(x) ) if pd.notnull(x) else x)

# cols_to_translate_google = ['domain','address','category','city' ,'country_name','description','email','name']
# for col in cols_to_translate_google:
#     google[col] = google[col].apply(lambda x: translate_text(str(x)) if pd.notnull(x) else x)

# cols_to_translate_website = ['root_domain','legal_name','main_city','site_name']
# for col in cols_to_translate_website:
#     website[col] = website[col].apply(lambda x: translate_text(str(x)) if pd.notnull(x) else x)


merged_df = pd.merge(
    facebook, 
    google, 
    how='outer', 
    left_on='name', 
    right_on='name', 
    suffixes=('_facebook', '_google')
) # name is the most complete unique identifier for facebook and google dataset. so i will join them on "name"

print(facebook)
print(google)

### filtering data in one column : as POT I will be choosing the data from the most complete dataset for that data.
merged_df['domain'] = merged_df['domain_facebook'].combine_first(merged_df['domain_google'])# priority for facebook df , google domains were redudant
merged_df['address'] = merged_df['address_google'].combine_first(merged_df['address_facebook'])# priority for google df
merged_df['category'] = merged_df['category'].combine_first(merged_df['categories']) # priority for google df
merged_df['city'] = merged_df['city_google'].combine_first(merged_df['city_facebook'])# priority for google
merged_df['country_code'] = merged_df['country_code_google'].combine_first(merged_df['country_code_facebook'])# priority for google df
merged_df['country_name'] = merged_df['country_name_google'].combine_first(merged_df['country_name_facebook']) # priority for google  df
merged_df['phone'] = merged_df['phone_google'].combine_first(merged_df['phone_facebook'])          ## google phone numbers were a stronger choice 
merged_df['zip_code'] = merged_df['zip_code_google'].combine_first(merged_df['zip_code_facebook']) ## priority for google
merged_df['description']  = merged_df['text'].combine_first(merged_df['description'])
merged_df['country_code'] = merged_df['region_code_google'].combine_first(merged_df['region_code_facebook'])
merged_df['region_name'] = merged_df['region_name_google'].combine_first(merged_df['region_name_facebook'])
merged_df['phone_country_code'] = merged_df['phone_country_code_google'].combine_first(merged_df['phone_country_code_facebook'])


# droping extra column from each side
merged_df = merged_df.drop(columns=[
    'domain_facebook', 'domain_google', 
    'address_facebook', 'address_google', 
    'categories', 'city_facebook', 'city_google', 
    'country_code_facebook', 'country_code_google',
    'country_name_facebook', 'country_name_google',
    'phone_facebook', 'phone_google',
    'zip_code_facebook', 'zip_code_google',
    'region_code_google','region_code_facebook',
    'phone_country_code_google','phone_country_code_facebook',
    'region_name_google','region_name_facebook',
    'text'
    
])

print(merged_df)
# print(website)
merged_df.to_csv('merged_df.csv', index=False)

duplicated_rows = merged_df[merged_df.duplicated(subset=['domain'], keep=False)]
print("duplicates")
print(duplicated_rows)

#between merged_df which is facebook and google merged, i will join with website on domain and root_domain(its more suitable for website and complete and yet strong unique indentifier)


merged_df2 = pd.merge(
    merged_df, 
    website, 
    how='outer', 
    left_on='domain', 
    right_on='root_domain', 
    suffixes=('_merged', '_website')
) 
merged_df2.to_csv('merged_df2.csv', index=False)

