import duckdb
from huggingface_hub import HfApi, DatasetCard, errors
from tqdm import tqdm
import argparse

con = duckdb.connect("./data/readmes.db")

api = HfApi()

def get_datasets():
    datasets_text = api.list_datasets(tags=["modality:text"], gated=False)
    datasets_text_gen = api.list_datasets(tags=["task_categories:text-generation"], gated=False)
    datasets_text_gen = [i for i in datasets_text_gen if i.downloads > 100]
    datasets_text = [i for i in datasets_text if i.downloads > 100]
    # Combine both dataset lists
    combined_datasets = datasets_text + datasets_text_gen

    # Create a dictionary to track unique datasets by ID
    unique_datasets = {}
    for dataset in combined_datasets:
        # Only keep the first occurrence of each dataset ID
        if dataset.id not in unique_datasets:
            unique_datasets[dataset.id] = dataset

    # Convert back to a list
    datasets = list(unique_datasets.values())
    return datasets


def populate_list_table(datasets):
    con.execute("CREATE TABLE IF NOT EXISTS datasets (id TEXT, name TEXT, downloads INTEGER, tags TEXT[], readme_collected BOOLEAN)")
    for dataset in datasets:
        # Pass parameters as a tuple
        params = (
            dataset.id, 
            dataset.id.split("/")[-1], 
            dataset.downloads, 
            dataset.tags, 
            False
        )
        con.execute(
            "INSERT INTO datasets (id, name, downloads, tags, readme_collected) VALUES (?, ?, ?, ?, ?)",
            params
        )


def populate_readme_table(datasets):
    con.execute("CREATE TABLE IF NOT EXISTS readmes (id TEXT, readme TEXT)")
    for dataset in tqdm(datasets):
        try:
            readme = DatasetCard.load(dataset).text
            con.execute("INSERT INTO readmes (id, readme) VALUES (?, ?)", (dataset, readme))
            con.execute("UPDATE datasets SET readme_collected = TRUE WHERE id = ?", (dataset,))
        except errors.EntryNotFoundError:
            print(f"Dataset {dataset} not found")
            con.execute("UPDATE datasets SET readme_collected = TRUE WHERE id = ?", (dataset,))

def get_datasets_needing_readme():
    con.execute("SELECT id FROM datasets WHERE readme_collected = FALSE")
    return [i[0] for i in con.fetchall()]


def main():
    parser = argparse.ArgumentParser(description='Initialize and populate dataset readme database')
    parser.add_argument('--init', action='store_true', 
                       help='Initialize database and populate dataset list')
    parser.add_argument('--collect-readmes', action='store_true',
                       help='Collect readmes for datasets that need them')
    
    args = parser.parse_args()
    
    if args.init:
        print("Initializing database and fetching dataset list...")
        datasets = get_datasets()
        populate_list_table(datasets)
        print("Database initialized with dataset list.")
        
    if args.collect_readmes:
        print("Collecting missing readmes...")
        datasets_needing_readme = get_datasets_needing_readme()
        if datasets_needing_readme:
            populate_readme_table(datasets_needing_readme)
            print("Readme collection complete.")
        else:
            print("No datasets need readme collection.")

if __name__ == "__main__":
    main()



