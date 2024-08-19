import pandas as pd
import numpy as np



def utility(csv_file_path) ->None:
    df=pd.read_csv(csv_file_path)
    
    pattern = r'^\d+\|\d+\|\d+ \d+\.\d+%$'

    # Apply correction only to values that don't match the correct format
    df.loc[~df["EndpointsBreakdown*,Compliance"].str.match(pattern, na=False), "EndpointsBreakdown*,Compliance"] = \
        df.loc[~df["EndpointsBreakdown*,Compliance"].str.match(pattern, na=False), "EndpointsBreakdown*,Compliance"].str.replace(r'(\d+\|\d+\|\d+) *(\d+\.\d+%)', r'\1 \2')
    

    df.rename(columns={"EndpointsBreakdown*,Compliance" : "Endpoints Breakdown(needed | failed | installed), Compliance"}, inplace=True)
    print(df.dtypes)
    df.to_csv('utility.csv',index=False)
    print(df.head)




def main():
    var=utility("extracted.csv")
    print(var)


if __name__ == "__main__":
    main()
