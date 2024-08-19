import os
import json
from google.protobuf.json_format import MessageToDict
import google
import numpy as np
import re
import cv2


# Set the path to the directory containing the poppler binaries
poppler_path = r"C:\Users\bsrijan\Documents\document_ai_pdf_extractor\myenv\Lib\site-packages\poppler-24.02.0\Library\bin"

# Add the poppler directory to the PATH environment variable
os.environ["PATH"] += os.pathsep + poppler_path





from typing import Optional
from google.api_core.client_options import ClientOptions
from google.cloud import documentai
import pandas as pd
from pdf2image import convert_from_path
from google.cloud.documentai_toolbox import document

# Set the path to your service account JSON key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "document_ai_service_account_key.json"

output_directory = "output"
output_file_prefix = "table"
#document_path = "./abc.json"

def process_pdf(pdf_path):
    # Split PDF into images
    images = split_pdf_into_images(pdf_path) #images is an array of individual image names like page_147.png

    # Initialize list to store table data
    all_table_data = []  #initializing an empty base df to store and keep appending each image's data as df till last image

    # Process each image
    for image_path in images:
        # Extract table data from image
        table_data = extract_table_data_from_image(image_path) #table_data contains df of table from a single image
        
        # Append extracted table data to list
        all_table_data.append(table_data)
    
    # Concatenate list of DataFrames into a single DataFrame
    #if all_table_data:
        #result_df = pd.concat(all_table_data, ignore_index=True)
    #else:
        #result_df = pd.DataFrame()  # Create an empty DataFrame if no data is available
    print('Tables in document:')
    return pd.concat(all_table_data, ignore_index=True)      #returning a big df which contains extracted table from page 147 till 210

def split_pdf_into_images(pdf_path, start_page=13):
    # Convert PDF to images using pdf2image library
    images = convert_from_path(pdf_path, first_page=start_page)
    image_paths = []
    for i, image in enumerate(images):
        image_path = f"page_{start_page + i}.png"
        image.save(image_path, "PNG")

        
        image_paths.append(image_path)
    return image_paths

def extract_table_data_from_image(image_path):
    '''with open(image_path, "rb") as image_file:
        content = image_file.read()
    
    # Process image using Document AI's Form Parser
    image = documentai.Document(
        content=content,
        mime_type="image/png"  # Specify the image mime type
    )'''

    # Process the document
    result_df = process_document("vision-api-416912", "us", "e9f47d2588fc52a2", image_path, "image/png")

    return result_df   #result_df contains datframe of table_data from a single image


def process_document(project_id: str, location: str, processor_id: str, file_path: str, mime_type: str, field_mask: Optional[str] = None, processor_version_id: Optional[str] = None) ->pd.DataFrame:
    # You must set the `api_endpoint` if you use a location other than "us".
    opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")
    client = documentai.DocumentProcessorServiceClient(client_options=opts)

    if processor_version_id:
        # The full resource name of the processor version
        name = client.processor_version_path(project_id, location, processor_id, processor_version_id)
    else:
        # The full resource name of the processor
        name = client.processor_path(project_id, location, processor_id)

    # Read the file into memory
    with open(file_path, "rb") as image:
        image_content = image.read()

    # Load binary data
    raw_document = documentai.RawDocument(content=image_content, mime_type=mime_type)

    # Define processing options
    process_options = documentai.ProcessOptions(
        # Process only specific pages
        individual_page_selector=documentai.ProcessOptions.IndividualPageSelector(pages=[1])
    )

    # Configure the process request
    request = documentai.ProcessRequest(
        name=name,
        raw_document=raw_document,
        field_mask=field_mask,
        process_options=process_options,
    )

    # Send the processing request
    result = client.process_document(request=request) #this result stores extracted table/text from a single image
    doc=result.document
    print(doc.text)
    # Get the Document object(json) from the result
    #document = result.document
    #print(document.text)


    # Serialize the Document object to a dictionary
    #document_dict = MessageToDict(result.document)
    #print(type(result.document))
    # Convert the dictionary to a JSON string
    document_json_string = google.cloud.documentai_v1.Document.to_json(result.document) #json of a single image file
    document_json=json.loads(document_json_string)
    #print(type(document_json_string))  class 'str'
    #print(type(document_json))         class 'dict'
    #print(document_json)
    # Specify the file path
    document_path = "google_json_for_each_image.json"

    # Create the file
    #with open(document_path, "w") as json_file:
    # Write an empty JSON object to initialize the file
     #json.dump({}, json_file)

    

    # Write JSON data to the file
    with open(document_path, "w") as json_file:
     json.dump(document_json, json_file)

    


    # Call table_sample function with the JSON representation of the document
    table_data = table_sample(document_path,output_directory,output_file_prefix)

    return table_data         #df (extracted from single image)from table_sample function is returned as table_data

def table_sample(document_path: str, output_directory: str, output_file_prefix: str) -> pd.DataFrame:
    wrapped_document = document.Document.from_document_path(document_path=document_path)
    all_dfs = []

    #print("Tables in Document")
    for page in wrapped_document.pages:
        for table_index, table in enumerate(page.tables):
            #try:
                # Convert table to Pandas DataFrame
                #print('try block')
                df = table.to_dataframe()
                all_dfs.append(df)

                output_filename = f"{output_file_prefix}-{page.page_number}-{table_index}.csv"

                # Ensure output directory exists
                os.makedirs(output_directory, exist_ok=True)

                # Write DataFrame to CSV file
                df.to_csv(os.path.join(output_directory, output_filename), index=False)

                # Write DataFrame to HTML file
                # df.to_html(os.path.join(output_directory, output_filename.replace('.csv', '.html')), index=False)

                # Write DataFrame to Markdown file
                # df.to_markdown(os.path.join(output_directory, output_filename.replace('.csv', '.md')), index=False)
            #except Exception as e:
                #print(f"Error processing table {table_index} on page {page.page_number}: {e}")

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    else:
        print("No tables found in the document.")
        return pd.DataFrame()  # Return an empty DataFrame if no tables are found
    


'''CREATING FUNCTION TO PROCESS EXTRACTED.CSV FILE INTO A DATAFRAME AND DO FOLLOWING OPERATIONS - 
   
   1)CHANGE NAME OF COLUMN D TO Endpoints Breakdown(needed | failed | installed), Compliance
   2)FIX 10685.71% AS 1 | 0 | 6 85.71%
   2)SEPARATE 150.00% AS 1 50.00%
   3)DUPLICATE COLUMN D AS COLUMN H NAMED Endpoints Breakdown
   4)ADD COLUMN I NAMED RANGE BASED ON COMPLIANCE %
   5)ADD COLUMN J NAMED EXEMPTION REASON BASED ON COLUMN D MOSTLY AND LITTLE BIT BASED ON COLUMN G'''


def convert_percentage_format(df, column_name):
    """
    Convert percentage format values in a specified column of a DataFrame to 'a|b|c d.dd%' format.

    Parameters:
        df (DataFrame): The input DataFrame.
        column_name (str): The name of the column with values to be converted.

    Returns:
        DataFrame: The DataFrame with converted values in the specified column.
    """
    def convert_value(value):
        # Extract the integer part and the decimal part of the percentage value
        integer_part, decimal_part = re.match(r'(\d+)(\.\d+)?%', value).groups()

        # Check if integer_part has at least 3 characters
        if len(integer_part) >= 3:
            # Adjust the integer part accordingly
            integer_part = f'{integer_part[0]}|{integer_part[1]}|{integer_part[2:]}'

        # Construct the converted format
        converted_value = f'{integer_part}{decimal_part}%'
        return converted_value

    # Apply the conversion function to the specified column
    df[column_name] = df[column_name].apply(lambda x: convert_value(x) if re.match(r'\d+(\.\d+)?%', str(x)) else x)

    return df


def fix_endpoints_format(df, column_name):
    """
    Fix the format of values in the specified column of a DataFrame.

    Parameters:
        df (DataFrame): The input DataFrame.
        column_name (str): The name of the column with values to be fixed.

    Returns:
        DataFrame: A new DataFrame with values in the specified column fixed.
    """
    # Create a copy of the original DataFrame to avoid modifying it
    #new_df = df.copy()

    # Define a regular expression pattern to capture the format
    pattern = r'(\d+\|\d+\|\d+) *(\d+[^\.]+)'

    # Define a function to fix the format of a single value
    def fix_value(value):
        if isinstance(value, str):
            return re.sub(pattern, r'\1 \2', value)
        return value

    # Apply the fix_value function to the specified column
    df[column_name] = df[column_name].apply(fix_value)
    return df



def fix_format(df, column_name):
    """
    Fix the format of values in the specified column of a DataFrame.

    Parameters:
        df (DataFrame): The input DataFrame.
        column_name (str): The name of the column with values to be fixed.

    Returns:
        DataFrame: A new DataFrame with values in the specified column fixed.
    """
    # Create a copy of the original DataFrame to avoid modifying it
    #new_df = df.copy()

    # Define a regular expression pattern to capture the format
    pattern = r'(\d+\|\d+\|\d+) *(\d+[^ ]+)'

    # Define a function to fix the format of a single value
    def fix_value(value):
        if isinstance(value, str):
            return re.sub(pattern, r'\1 \2', value)
        return value

    # Apply the fix_value function to the specified column
    df[column_name] = df[column_name].apply(fix_value)
    return df




def extract_last_five(string):
    if isinstance(string, str):
        # Extract the last 5 characters of the string
        last_five = string[-8:]
        # Capture the first 3 characters of the string
        first_three = last_five[:4]
        return first_three
    else:
        return str(string)  # Convert non-string value to string and return

def swap_characters(string, index1=-7, index2=-8):
    try:
        if isinstance(string, str):
            # Convert the string to a list of characters
            string_list = list(string)
            
            # Swap the characters at the given indexes
            string_list[index1], string_list[index2] = string_list[index2], string_list[index1]
            
            # Convert the list back to a string
            swapped_string = ''.join(string_list)
            
            return swapped_string
        else:
            return str(string)  # Convert non-string value to string and return
        
    except IndexError as e:
        print("Error:", e)
        return None




#adding range column
def extract_percentage(string):
    # Ensure the input is a string
    if isinstance(string, str):
        # Split the string to extract the percentage part
        parts = string.split()
        if len(parts) >= 2:
            percentage_str = parts[-1]  # Extract the last part which should be the percentage
            try:
                # Remove '%' sign and convert to float
                percentage = float(percentage_str.strip('%'))
                return percentage
            except ValueError:
                print(f"Could not convert '{percentage_str}' to a float.")
                return None
        else:
            return None
    else:
        return None


def categorize_percentage(percentage):
    if percentage is not None:
        if percentage < 70:
            return '<70%'
        elif 70 <= percentage <= 80:
            return '71-80%'
        elif 81 <= percentage <= 90:
            return '81-90%'
        elif 91 <= percentage <= 100:
            return '91-100%'
    else:
        return None    


def main():
    pd.set_option('future.no_silent_downcasting', True) 

    pdf_path = "RP-2501-Remediation_Plan_Compliance_Report_2024-03-09_13-04.pdf"
    all_table_data = process_pdf(pdf_path)
    print(all_table_data) #returns a df from start page of pdf to last page pdf

    #now converting this df to csv file 
    all_table_data.to_csv('extracted.csv',index=False)


    '''Now working on the extracted document'''
    df=pd.read_csv('extracted.csv')
    df=convert_percentage_format(df, 'EndpointsBreakdown*,Compliance')
    new_df=df.copy()
    new_df = fix_endpoints_format(new_df, 'EndpointsBreakdown*,Compliance')
    new_df = fix_format(new_df, 'EndpointsBreakdown*,Compliance')

    # Iterate over values in 'col1' and apply functions
    for i, val in new_df['EndpointsBreakdown*,Compliance'].items():

        result = extract_last_five(val)
        if result == '1 00':

            result = swap_characters(val, index1=-7, index2=-8)
            if result is not None:

               new_df.at[i, 'EndpointsBreakdown*,Compliance'] = result

    # Apply the functions to the column and create a new column for the categories
    new_df['percentage'] = new_df['EndpointsBreakdown*,Compliance'].apply(extract_percentage)
    new_df['Range'] = new_df['percentage'].apply(categorize_percentage)
    #print(new_df)
    new_df.drop(columns=['percentage'], inplace=True)

    '''ADDING SEVERAL EXEMPTION REASONS USING PREDEFINED CONDITIONS'''

    new_df['Exemption'] = new_df['Exemption'].replace('', np.nan)

    # Add a new column 'Exemption Reason'
    new_df['Exemption Reason'] = ''

    #insert_index = new_df.columns.get_loc('Range') + 1
    #new_df.insert(insert_index, 'Exemption Reason', None)
    # Replace any NaN values in the result of str.contains() with False
    contains_untested_endpoint = new_df['Exemption'].str.contains('Untested Endpoint').fillna(False)
    contains_low_disc_space = new_df['Exemption'].str.contains('Low Disc Space').fillna(False)

    # Use the modified result to assign new values to the 'Exemption Reason' column
    new_df.loc[contains_untested_endpoint, 'Exemption Reason'] = 'Connector needs to be installed'
    new_df.loc[contains_low_disc_space, 'Exemption Reason'] = 'Low Disk Space'

    # use str.startswith() to check if the string starts with '0|0|'
    contains_need_to_analyze = new_df['EndpointsBreakdown*,Compliance'].str.startswith('0|').fillna(False)

    # Use the modified result to assign new values to the 'Exemption Reason' column
    new_df.loc[contains_need_to_analyze, 'Exemption Reason'] = 'Need to analyze the issue'

    # use str.startswith() to check if the string starts with '0|0|'
    contains_fully_patched = new_df['EndpointsBreakdown*,Compliance'].str.startswith('0|0|').fillna(False)

    # Use the modified result to assign new values to the 'Exemption Reason' column
    new_df.loc[contains_fully_patched, 'Exemption Reason'] = 'Fully Patched'

    #exemption reason is 'disconnected' when the installed is 0
    '''new_df['Exemption Reason'] = new_df.apply(lambda row: 'Disconnected' 
                                              if isinstance(row['EndpointsBreakdown*,Compliance'], str) 
                                              and row['EndpointsBreakdown*,Compliance'].split('|')[2][0] == '0' 
                                              else row['Exemption Reason'], axis=1)'''
    
    new_df['Exemption Reason'] = new_df.apply(lambda row: 'Disconnected' 
                                              if isinstance(row['EndpointsBreakdown*,Compliance'], str) 
                                              and len(row['EndpointsBreakdown*,Compliance'].split('|')) >= 3 
                                              and row['EndpointsBreakdown*,Compliance'].split('|')[2][0] == '0' 
                                              else row['Exemption Reason'], axis=1)
    
    #again rewriting those values which might have been changed during the above manipulation
    #insert_index = new_df.columns.get_loc('Range') + 1
    #new_df.insert(insert_index, 'Exemption Reason', None)
    # Replace any NaN values in the result of str.contains() with False
    contains_untested_endpoint = new_df['Exemption'].str.contains('Untested Endpoint').fillna(False)
    contains_low_disc_space = new_df['Exemption'].str.contains('Low Disc Space').fillna(False)

    # Use the modified result to assign new values to the 'Exemption Reason' column
    new_df.loc[contains_untested_endpoint, 'Exemption Reason'] = 'Connector needs to be installed'
    new_df.loc[contains_low_disc_space, 'Exemption Reason'] = 'Low Disk Space'
    new_df['Exemption Reason'] = new_df['Exemption Reason'].replace('', 'Maintenance Window Expired', regex=True)

    
    
    
    new_df.to_csv('new13.csv')
    print('PDF converted to csv successfully!')



    

if __name__ == "__main__":
    main()






# Example usage
