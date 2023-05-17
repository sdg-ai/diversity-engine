import time
import sys
from pathlib import Path
from core.s3_utilities import *
sys.path.append(str(Path(__file__).resolve().parents[2]))
from diversitymaster import diversity

# all_files = list_files_folders()
all_raw_company_data = list_files_in_raw_data_folder()
exclude = ['raw_data/scorecards.json',
           'raw_data/scorecards_tst1.json', 'raw_data/scorecards_tst2.json']
print(all_raw_company_data)
# all_raw_company_data = [el for el in all_files[0]
#                        if el.startswith("raw_data/") and el not in exclude]

SERVICE_ENDPOINT = os.getenv(
    "SERVICE_ENDPOINT", "https://s3.us-west-1.wasabisys.com")
ACCESS_KEY_ID = os.environ.get("ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.environ.get("SECRET_ACCESS_KEY")


def run_scorecard_for_company_and_save_data(path: str):
    company_name = path.split("/")[1].split(".jsonl.gz")[0].lower()
    # if path_to_save in list(all_raw_company_data)[0]:
    #    pass
    # else:
    print(path)
    print(company_name)

    # load the data from s3
    raw_dat = read_jsonl_file(
        service_endpoint=SERVICE_ENDPOINT,
        access_key_id=ACCESS_KEY_ID,
        secret_access_key=SECRET_ACCESS_KEY,
        bucket_name="dei-bucket",
        object_path=path,
        return_lines=True
    )
    N = 100_000
    sc = diversity.scorecard.Scorecard()
    sc.generate_counts((line for line in raw_dat),
                        comp_list=[company_name], limit=N)
    sc.create_scorecards()
    res = sc.get_companies()
    print(res)
    if len(list(res.keys())) > 0:
        print(f"companies: {res.keys()}")
        comp_dict = res[list(res.keys())[0]]

        # remove useless data
        for year in [el for el in comp_dict.keys() if el != "score"]:
            for key in list(comp_dict[year].keys()):
                if 'role' in key:
                    del comp_dict[year][key]

        # save the data to s3
        save_dict_to_s3_as_jsonl_file(
            data_dict=comp_dict,
            service_endpoint=SERVICE_ENDPOINT,
            access_key_id=ACCESS_KEY_ID,
            secret_access_key=SECRET_ACCESS_KEY,
            bucket_name="dei-bucket",
            object_path=f"company_scores/{company_name}.jsonl.gz"
        )
    else:
        pass
    print("")
    return


def run_all_scores():
    for i, company_path in enumerate(all_raw_company_data):
        print(i)
        start = time.time()
        run_scorecard_for_company_and_save_data(path=company_path)
        end = time.time()
        print(f"Completed in {end-start} seconds \n")


if __name__ == "__main__":
    run_all_scores()
    # run_scorecard_for_company_and_save_data(
    #    path="raw_data/Activision_Blizzard.jsonl.gz")
