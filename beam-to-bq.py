import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from google.cloud import storage
import json

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

            # Check horizontal
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

def determine_winner_from_file(file_name):
    game_results = []

    try:
        #Create a GCS client
        storage_client = storage.Client()

        #Access the GCS bucket and file
        bucket_name = "connect-four-us"
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

p = beam.Pipeline()

(
    p
    | "Create game results" >> beam.Create(game_results)
    | "Write to BigQuery" >> WriteToBigQuery(
        table="project_id:dataset.table",
        schema="game_number:INTEGER, winner_id:STRING, loser_id:STRING",
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        custom_gcs_temp_location="gs://your-bucket/temp"
    )
)

p.run()
