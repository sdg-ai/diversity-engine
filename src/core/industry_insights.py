# industry insights

import pandas as pd
import logging
import json
from core.s3_utilities import read_jsonl_file, save_dict_to_s3_as_jsonl_file

logger = logging.getLogger(__name__)


def get_all_companies(
    service_endpoint: str,
    access_key_id: str,
    secret_access_key: str,
    bucket_name: str,
):
    """
    get the metadata about companies:
    - company name
    - industry
    - sector
    - company size
    - company website
    - company id
    """
    all_dat = read_jsonl_file(
        service_endpoint=service_endpoint,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        bucket_name=bucket_name,
        object_path="company_scores/all/companies_metadata.jsonl.gz",
        return_lines=True
    )
    result_dict = {}
    for d in all_dat:
        result_dict.update(json.loads(d))
    return result_dict


class IndustryInsights:
    """
    Get the top companies by industries
    """

    def __init__(
        self,
        service_endpoint: str,
        access_key_id: str,
        secret_access_key: str,
        bucket_name: str = "dei-bucket",
    ):
        self.service_endpoint = service_endpoint
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.bucket_name = bucket_name

    def get_data_across_companies(self)->pd.DataFrame:
        """
        returns the content of a file stored in the bucket
        """
        comp_dict = get_all_companies(
            service_endpoint=self.service_endpoint,
            access_key_id=self.access_key_id,
            secret_access_key=self.secret_access_key,
            bucket_name=self.bucket_name,
        )
        
        # Initialize list to store flattened data
        flattened_data = []

        for _, company_data in comp_dict.items():
            # Flatten the inner dictionary and append to the list
            scores = company_data.get('scores', {}).get('score', {})
            flattened_data.append({
                "company_name": company_data.get('company_name'),
                "industry": company_data.get('industry'),
                "sector": company_data.get('sector'),
                "company_size": company_data.get('company_size'),
                "company_website": company_data.get('company_website'),
                "company_id": company_data.get('company_id'),
                "Talent Pipeline Score": scores.get('Talent Pipeline'),
                "Retention Score": scores.get('Retention'),
                "Access & Advancement Score": scores.get('Access & Advancement'),
                "Representation Score": scores.get('Representation'),
                "Historical Score": scores.get('Historical'),
            })

        # Convert to pandas DataFrame
        df = pd.DataFrame(flattened_data)
        nb_industries = df["industry"].nunique()
        nb_sectors = df["sector"].nunique()
        logger.info(f"Number of distinct industries: {nb_industries}")
        logger.info(f"Number of distinct sectors: {nb_sectors}")
        return df
    
    def industry_and_sector_comparisons(self, df: pd.DataFrame)->pd.DataFrame:
        # Rank by industry for each score
        for score in ["Talent Pipeline Score", "Retention Score", "Access & Advancement Score", "Representation Score", "Historical Score"]:
            df[f"{score} Rank by Industry"] = df.groupby("industry")[score].rank(ascending=False, method='min')
            df[f"{score} Rank by Sector"] = df.groupby("sector")[score].rank(ascending=False, method='min')

        # Determine top % by industry and sector for each score
        for score in ["Talent Pipeline Score", "Retention Score", "Access & Advancement Score", "Representation Score", "Historical Score"]:
            total_companies_in_industry = df.groupby('industry').size()
            total_companies_in_sector = df.groupby('sector').size()

            df[f"{score} Top % by Industry"] = df.apply(lambda x: (x[f"{score} Rank by Industry"] / total_companies_in_industry[x['industry']]) * 100, axis=1)
            df[f"{score} Top % by Sector"] = df.apply(lambda x: (x[f"{score} Rank by Sector"] / total_companies_in_sector[x['sector']]) * 100, axis=1)
        return df
    
    
    def industry_statistics(self, companies_df: pd.DataFrame)->pd.DataFrame:
        # Industry Average and Median
        industry_avg = companies_df.groupby('industry')[
            ["Talent Pipeline Score", "Retention Score", "Access & Advancement Score", "Representation Score", "Historical Score"]
        ].mean().rename(columns=lambda x: x + ' Industry Average')

        industry_median = companies_df.groupby('industry')[
            ["Talent Pipeline Score", "Retention Score", "Access & Advancement Score", "Representation Score", "Historical Score"]
        ].median().rename(columns=lambda x: x + ' Industry Median')

        industry_summary = pd.concat([industry_avg, industry_median], axis=1)
        return industry_summary
    
    
    def sector_statistics(self, companies_df: pd.DataFrame)->pd.DataFrame:
        # Sector Average and Median
        sector_avg = companies_df.groupby('sector')[
            ["Talent Pipeline Score", "Retention Score", "Access & Advancement Score", "Representation Score", "Historical Score"]
        ].mean().rename(columns=lambda x: x + ' Sector Average')

        sector_median = companies_df.groupby('sector')[
            ["Talent Pipeline Score", "Retention Score", "Access & Advancement Score", "Representation Score", "Historical Score"]
        ].median().rename(columns=lambda x: x + ' Sector Median')

        sector_summary = pd.concat([sector_avg, sector_median], axis=1)
        return sector_summary
    
    
    def run_and_save_industry_and_sector_stats(self):
        df = self.get_data_across_companies()
        
        # industry statistics
        ind_df = self.industry_statistics(companies_df=df)
        ind_df = ind_df.fillna(0)
        ind_df[ind_df<0] = 0
        ind_dict = ind_df.to_dict()
        logger.info(ind_dict)
        save_dict_to_s3_as_jsonl_file(
            data_dict=ind_dict,
            service_endpoint=self.service_endpoint,
            access_key_id=self.access_key_id,
            secret_access_key=self.secret_access_key,
            bucket_name="dei-bucket",
            object_path=f"company_scores/all/industry_statistics.jsonl.gz"
        )
        logger.info("industry stats saved on wasabi")
        # sector statistics
        sec_df = self.sector_statistics(companies_df=df)
        sec_df = sec_df.fillna(0)
        sec_df[sec_df<0] = 0
        sec_dict = sec_df.to_dict()
        logger.info(sec_dict)
        save_dict_to_s3_as_jsonl_file(
            data_dict=sec_dict,
            service_endpoint=self.service_endpoint,
            access_key_id=self.access_key_id,
            secret_access_key=self.secret_access_key,
            bucket_name="dei-bucket",
            object_path=f"company_scores/all/sector_statistics.jsonl.gz"
        )
        logger.info("sector stats saved on wasabi")


def main():
    return

if __name__ == "__main__":
    main()
