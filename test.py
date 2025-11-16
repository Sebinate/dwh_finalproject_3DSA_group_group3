from scripts.utils import ingest_utils

# for batch in ingest_utils.csv_reader(r"C:\Users\User\dwh_finalproject_3DSA_group_group3\data\Project Dataset-20241024T131910Z-001\Marketing Department\transactional_campaign_data.csv"):
#     print(batch)

print(ingest_utils.xlsx_reader(r"C:\Users\User\dwh_finalproject_3DSA_group_group3\data\Project Dataset-20241024T131910Z-001\Business Department\product_list.xlsx"))