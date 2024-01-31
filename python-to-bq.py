import csv
from google.cloud import storage
from datetime import datetime
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
import json

#Parsing the match data into player names and moves
def parse_match(match_data):
    player_names, moves = match_data.split("\n")
    players = player_names.split(",")
    board = [[" " for _ in range(7)] for _ in range(6)]
  
    #Iterate over each move and update the board
    for move in moves.split(","):
        color = move[0]
        column = int(move[1]) - 1
        
        row = 5
        while row >= 0 and board[row][column] != " ":
            row -= 1
        
        board[row][column] = color
    
    return players, board

#Find the next empty row in the specified column of the grid
def find_next_empty_row(grid, col):
    for row in range(5, -1, -1):
        if grid[row][col] == ' ':
            return row
    return None

#Check all cells in the grid for a win condition
def check_winner(grid):
    for row in range(6):
        for col in range(7):
            player = grid[row][col]
            if player == ' ':
                continue

            #Check horizontal
            if col <= 3 and all(grid[row][c] == player for c in range(col, col + 4)):
                return True

            #Check vertical
            if row <= 2 and all(grid[r][col] == player for r in range(row, row + 4)):
                return True

            #Check diagonal (top-left to bottom-right)
            if row <= 2 and col <= 3 and all(grid[row + d][col + d] == player for d in range(4)):
                return True

            #Check diagonal (bottom-left to top-right)
            if row >= 3 and col <= 3 and all(grid[row - d][col + d] == player for d in range(4)):
                return True

    return False


def determine_winner(moves_list):
    game_results = []

    for moves in moves_list:
        grid = [[' ' for _ in range(7)] for _ in range(6)]
        player_ids = moves[0].split(",")  #Extract player IDs

        for i, move in enumerate(moves[1].split(","), start=1):
            if len(move) < 2:
                continue  #Skip invalid moves

            try:
                col = int(move[1]) - 1
                row = find_next_empty_row(grid, col)

                if i % 2 == 0:
                    player_id = player_ids[1]
                    token = 'R'
                else:
                    player_id = player_ids[0]
                    token = 'B'

                grid[row][col] = token

                if check_winner(grid):
                    game_results.append((player_id, player_ids[0] if i % 2 == 0 else player_ids[1]))
                    break
            except ValueError:
                continue  #Skip invalid moves

    return game_results


def determine_winner_from_file(file_name):
    game_results = []
    
    #Create a GCS client
    storage_client = storage.Client()

    try:
        #Access the GCS bucket and file
        bucket_name = "your-bucket"
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)

        #Download and read the file contents
        data = blob.download_as_text()

        matches = data.strip().split("\n\n")

        for match in matches:
            match_lines = match.strip().split("\n")
            if len(match_lines) >= 2:
                moves = match_lines[1:]
                winners = determine_winner([match_lines])

                if winners:
                    game_results.append(winners)

    except Exception as e:
        print(f"Error accessing GCS file: {e}")

    return game_results

#Specify the file name
file_name = 'matchdata.txt'

# Get the game results from the file
game_results = determine_winner_from_file(file_name)

#Print the game results in tabular format
print("Game\tWinner ID\tLoser ID")
for game_number, winners in enumerate(game_results, start=1):
    winner_id, loser_id = winners[0]
    if loser_id:
        print(f"{game_number}\t{winner_id}\t\t{loser_id}")
    else:
        print(f"{game_number}\t{winner_id}\t\t-")

#Create a BigQuery client
client = bigquery.Client()

#Specify the project ID, dataset ID, and table name
project_id = "project-id"
dataset_id = "your-dataset"
table_name = "your-table"

#Create the dataset if it does not exist
dataset_ref = client.dataset(dataset_id, project=project_id)

try:
    dataset = client.get_dataset(dataset_ref)
    print(f"The dataset '{dataset_id}' already exists in BigQuery.")
except Exception as e:
    print(f"The dataset '{dataset_id}' does not exist. Creating a new dataset...")
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    dataset = client.create_dataset(dataset)
    print(f"Dataset '{dataset_id}' created successfully in BigQuery.")

#Define the schema for the table
schema = [
    SchemaField("game_number", "INT64"),
    SchemaField("winner_id", "STRING"),
    SchemaField("loser_id", "STRING")
]

#Create the table if it does not exist
table_ref = dataset.table(table_name)

try:
    table = client.get_table(table_ref)
    print(f"The table '{table_name}' already exists in BigQuery.")
except Exception as e:
    print(f"The table '{table_name}' does not exist. Creating a new table...")
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)
    print(f"Table '{table_name}' created successfully in BigQuery.")

#Create the BigQuery table reference
table_ref = client.dataset(dataset_id, project=project_id).table(table_name)

#Load the game results into the table
rows_to_insert = []

for game_number, winners in enumerate(game_results, start=1):
    winner_id, loser_id = winners[0]
    rows_to_insert.append((game_number, winner_id, loser_id))

#Convert the game results to JSON format
rows_to_insert_json = []

for game_number, winners in enumerate(game_results, start=1):
    winner_id, loser_id = winners[0]
    row_data = {
        "game_number": game_number,
        "winner_id": winner_id,
        "loser_id": loser_id
    }
    rows_to_insert_json.append(row_data)

#Create the BigQuery load job configuration
job_config = bigquery.LoadJobConfig(schema=schema)

#Start the BigQuery load job
load_job = client.load_table_from_json(rows_to_insert_json, table_ref, job_config=job_config)

#Wait for the load job to complete
load_job.result()

#Check the load job status
if load_job.state == "DONE":
    print(f"Game results loaded successfully into table '{table_name}' in BigQuery.")
else:
    print(f"Error loading game results into table '{table_name}' in BigQuery.")
