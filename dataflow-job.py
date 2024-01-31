import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from google.cloud import storage
import json
import argparse

#Function to find the next empty row in a column of a grid
def find_next_empty_row(grid, col):
    for row in range(5, -1, -1):
        if grid[row][col] == ' ':
            return row
    return None

#Function to check if there is a winner in the grid
def check_winner(grid):
    for row in range(6):
        for col in range(7):
            player = grid[row][col]
            if player == ' ':
                continue

            if col <= 3 and all(grid[row][c] == player for c in range(col, col + 4)):
                return True

            if row <= 2 and all(grid[r][col] == player for r in range(row, row + 4)):
                return True

            if row <= 2 and col <= 3 and all(grid[row + d][col + d] == player for d in range(4)):
                return True

            if row >= 3 and col <= 3 and all(grid[row - d][col + d] == player for d in range(4)):
                return True

    return False

#Function to determine the winner(s) of each game based on the moves
def determine_winner(moves_list):
    game_results = []

    for moves in moves_list:
        grid = [[' ' for _ in range(7)] for _ in range(6)]
        player_ids = moves[0].split(",")

        for i, move in enumerate(moves[1].split(","), start=1):
            if len(move) < 2:
                continue

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
                    if i % 2 == 0:
                        winner = player_ids[1]
                        loser = player_ids[0]
                    else:
                        winner = player_ids[0]
                        loser = player_ids[1]

                    game_results.append((winner, loser))
                    break
            except ValueError:
                continue

    return game_results

#Function to determine the winner(s) from a file
def determine_winner_from_file(file_name):
    game_results = []

    try:
        #Create a GCS client
        storage_client = storage.Client()

        #Access the GCS bucket and file
        bucket_name = "your-bucket"
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(file_name)

        #Download and read the file contents
        data = blob.download_as_text()

        matches = data.strip().split("\n\n")

        for game_number, match in enumerate(matches, start=1):
            match_lines = match.strip().split("\n")
            if len(match_lines) >= 2:
                moves = match_lines[1:]
                winners = determine_winner([match_lines])

                if winners:
                    for winner, loser in winners:
                        game_results.append({"game_number": game_number, "winner_id": winner, "loser_id": loser})

    except Exception as e:
        print(f"Error accessing GCS file: {e}")

    return game_results

#Specify the file name
file_name = "matchdata.txt"

#Get the game results from the file
game_results = determine_winner_from_file(file_name)

#Function to run the Beam pipeline
def run_pipeline(project, region, bucket, file_name, table):
    game_results = determine_winner_from_file(file_name)

    options = {
        "project": project,
        "region": region,
        "staging_location": f"gs://{bucket}/staging",
        "temp_location": f"gs://{bucket}/temp",
        "job_name": "connect-four-job",
        "runner": "DataflowRunner",
        "save_main_session": True,
    }

    pipeline_options = beam.pipeline.PipelineOptions(flags=[], **options)

    p = beam.Pipeline(options=pipeline_options)

    (
        p
        | "Create game results" >> beam.Create(game_results)
        | "Write to BigQuery" >> WriteToBigQuery(
            table=table,
            schema="game_number:INTEGER, winner_id:STRING, loser_id:STRING",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )

    p.run().wait_until_finish()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", help="Google Cloud project ID")
    parser.add_argument("--region", help="Dataflow region")
    parser.add_argument("--bucket", help="Google Cloud Storage bucket")
    parser.add_argument("--file", help="Input file name")
    parser.add_argument("--table", help="BigQuery table")

    args = parser.parse_args()

    run_pipeline(args.project, args.region, args.bucket, args.file, args.table)
