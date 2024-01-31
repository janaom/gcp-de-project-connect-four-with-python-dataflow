
# <img width="40" alt="image" src="https://github.com/janaom/gcp-data-engineering-etl-with-composer-dataflow/assets/83917694/60f8f158-3bdc-4b3d-94ae-27a12441e2a3">  GCP Data Engineering project: Connect Four game with Python and Apache Beam 🔴⚫


Get ready for an exciting adventure in the world of Connect Four!🥳 I have an awesome task for you: we've got 11 players, 88 games, and a text file filled with all their moves. Your mission is to identify the winners and store the results in a database to enable future analytical queries. Sounds like an easy task, right?

But here's the twist: the winners are unknown!🤨 Participants continue playing until the game grid is completely filled with chips. So your task is to parse the match data (filename: matchdata.txt) and figure out who won the match. And when you have all the data available, the final step is to create a similar table with the following structure:

```
+-------------+------------+--------------+-----+------+-----------------+
| player_rank | player_id  | games_played | won | lost | win_percentage  |
+-------------+------------+--------------+-----+------+-----------------
```

I will guide you through the process of transforming the Connect Four algorithm into a standalone Python code and subsequently converting it into Apache Beam code to create an ETL Dataflow pipeline. The main focus here is to showcase the flexibility of Apache Beam and demonstrate how you can easily adapt your Python code with any complex logic to create a robust ETL pipeline. ETL, in this context, goes beyond data cleaning and removing invalid characters.

I will provide a step-by-step guide on how to query your data in BigQuery to create the main table. Additionally, we will explore creating a Looker report to visualize the Connect Four game analytics, setting up scheduled email deliveries to each player based on their player_id, and even building a Slack App to automatically share the saved CSV file with the game results in a designated Slack channel.

I use GCP services for this project.

![connect-four2 drawio](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/cc5626fa-8d11-449a-874f-2c64910638a9)


<img width="20" alt="image" src="https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/0887957d-db1b-4938-a9fa-f497fcebbeff"> Google Cloud Storage: it is used to store the text file, providing scalable and durable object storage.


<img width="20" alt="image" src="https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/5df502de-1936-42fb-a3d5-2fa7cf0c5723"> Dataflow: it is utilized to extract data from the storage bucket, perform data transformations such as counting winners and losers, and load the processed data into BigQuery.


<img width="20" alt="image" src="https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/7c95b56f-a1fc-49b3-8e96-ed607f2094ea"> BigQuery: it serves as the data repository for the Connect Four project, enabling efficient querying and analysis of the dataset. 


<img width="20" alt="image" src="https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/1d47b6a1-a957-4156-89f5-1cbbe5d72451"> Slack Integration: it is used to send the report results via Slack. This integration facilitates real-time sharing and collaboration with team members, ensuring efficient communication and discussion of the Connect Four game results.


<img width="20" alt="image" src="https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/512b60f2-f910-46f8-b785-563c92ca70c4"> Looker: it is used to generate insightful reports and visualizations based on the data in BigQuery, facilitating data exploration and analysis.


✉️Email Scheduler: it is used to schedule the delivery of Looker reports via email. This feature ensures regular and automated delivery of the Connect Four game reports to the intended recipients, keeping them updated on the latest analysis and insights.

# Connect Four rules

Before we get to the task, let's review the rules of Connect Four. In this game, two players take turns strategically placing red and black chips on a 6x7 grid. Each player is assigned a color, and red always goes first. The goal is to be the first player to connect four of their chips in a horizontal, vertical, or diagonal line on the grid.

The data is in this format:

```
player_0, player_1
R1,B1,R2,B2,R3,B3,R4,B6,...

player_2, player_3
R1,B2,R3,B1,R4,...
```

In the Connect Four matches, the first player listed always plays as red, while the second player always plays as black. The moves are represented using a combination of the color and the column number: `<color><column>`. In the first match above, player_0 makes the move "R1" which denotes that they place their chip in the first column. Since there are no chips in that column, it falls to the bottom. player_1 (black) responds by placing their chip in the first column as well. However, since there's already a red chip in that column, the black chip ends up on top of the red chip.

Each game contains two rows:

```
Row 1: player names 
Row 2: moves played in the game
```

Keep in mind that the game could be over before the final move recorded in the file is made. Once you have identified the winning move, there is no need to continue reading the data. It's important to consider that there are no draw cases (if the board becomes completely filled without either player achieving a four-in-a-row connection, the game is considered a draw).

Here is an example from the second game in the text file with R1,B2,R5 moves. Player_1 secured the win with the winning move on R5. Here, R (red) moves, B (yellow) moves.


player_1,player_6 R3,B3,R4,B6,R1,B5,R6,B3,R4,B4,R6,B1,R5,B7,R6,B2,R2,B7,`R1,B2,R5`,B1,R4,B3,R2,B2,R1,B4,R3,B5,R5,B5,R3,B7,R2,B1,R7,B6,R7,B6,R7,B4

![20240126_185845](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/ac44a70e-987f-4079-8356-9d9168da8de6)

# Connect Four algorithm

First, let's discuss the Connect Four algorithm.

```python
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
```
- The `find_next_empty_row` function takes a grid and a column as input and searches for the next empty row in that column. It iterates through the rows in reverse order and returns the index of the first empty row found. If no empty row is found, it returns `None`.
  
- The `check_winner` function  meticulously examines the grid to determine if there is a win condition.  It checks for four consecutive cells with the same player value in  horizontal, vertical, and diagonal directions. If any win condition is  encountered, the function returns `True`; otherwise, it returns `False`.
  
- The `determine_winner` function takes a list of moves as input and determines the winner of each game. It initializes an empty grid and extracts the player IDs from the first element of each moves list. It then iterates through the moves, updating the grid with the corresponding player's token. After each move, it checks if a winning condition is met using the `check_winner` function. If a winner is found, it appends a tuple of the winning player's ID and the opponent's ID to the `game_results` list. Finally, it returns the `game_results` list, which contains the winners for each game in the moves list.

# Python code

Upload matchdata.txt file to the bucket.

![image](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/38bc6d6a-c33a-445f-8f95-b9164eaf196e)

Analyze the Python code `python-to-bq.py` and run it with the command `python python-to-bq.py`

Additionally to the Connect Four algorithm, this code has these elements:

✔️ `determine_winner_from_file(file_name)` function takes a `file_name` as input and returns a list of game results

✔️ the code reads the txt file from the bucket

✔️ it iterates over the `game_results` list and prints the game number, winner ID, and loser ID in a tabular format
  
✔️ the code creates a dataset and table in BigQuery if they do not exist
  
✔️ the game results are then loaded into the table

![20240126_210128](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/b8a13b2d-3ffb-4cba-896c-8945d0f74129)

The game results are loaded into the BigQuery table.

![image](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/6f1571f8-0cf1-4707-ab19-e72b0c6da60d)


To order the data by the `game_number` column, you can use the `ORDER BY` clause.

```SQL
SELECT game_number, winner_id, loser_id
FROM your_table_name
ORDER BY game_number;
```

You'll be able to view the results of all 88 games.

![image](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/e99e79c9-c1aa-447d-85b8-df2df84ca78f)



# Beam code

Let's highlight the primary distinction between Beam and Python code.

```python
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
```

✔️ a Beam pipeline (`p`) is created.

✔️ the `game_results` list is passed to the pipeline using the `beam.Create` transform, which creates a PCollection containing the game results.

✔️ the `WriteToBigQuery` transform is  applied to the PCollection of game results. It specifies the BigQuery  table to write to, the schema of the table, the write disposition  (appending to the existing table), and the temporary location in Google  Cloud Storage.

In this case the code assumes that the dataset already exists in BigQuery. 

To execute the code, simply run the command `python beam-to-bq.py`. Executing this command will yield identical results to running the Python code. To verify all game results in a sequential order, run the corresponding SQL query.

```SQL
SELECT game_number, winner_id, loser_id
FROM your_table_name
ORDER BY game_number;
```

# Dataflow job

The last step is to adjust Beam code to the Dataflow job. In this code, the assumption is that the dataset already exists in BigQuery. The code has specific imports and details.

```python
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
```

Create Dataflow job with this command:

```python
python dataflow.py \
  --project project_id \
  --region region \
  --bucket your-bucket \
  --file matchdata.txt \
  --table project_id:dataset.table
```
Here is an example: `python dataflow-job.py --project connect-four-408317 --region us-central1 --bucket connect-four-us --file matchdata.txt --table connect-four-408317:connect_four.dataflow_results`

![image](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/c01a49d3-7cce-4f4e-b211-b719b5d3b361)


❗️ My first job failed with the comment: 'The zone 'projects/connect-four-408317/zones/us-central1-b' does not have enough resources available to fulfill the request. Try a different zone, or try again later.' So I changed us-central1 to us-east1.

You will see a simple pipeline. And then a table in BigQuery.

![20240128_182806](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/5a955dbc-744a-4f5f-ac06-06cc66bb02cc)

The `help` parameter in `argparse.ArgumentParser().add_argument()` is used to provide a brief description or help message for the command-line argument. It serves as a helpful guide for users who may not be familiar with the script or its command-line interface.

When users run a script with the `--help` flag, argparse will automatically generate a help message that includes the descriptions specified by the `help` parameter for each argument. This helps users understand the purpose and expected values of each argument.

To see an example, run: `python dataflow.py --help`

![image](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/b497f157-10c5-48ee-a193-1f8967a71dbf)


# A step-by-step guide to data querying

And now that we have all the results stored in BigQuery, let's return to our primary objective and proceed with creating the main table.

```
+-------------+------------+--------------+-----+------+-----------------+
| player_rank | player_id  | games_played | won | lost | win_percentage  |
+-------------+------------+--------------+-----+------+-----------------
```

In our current schema we have: `game_number`, `winner_id`, `loser_id` fields.

![image](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/f13c8b20-e1ed-4059-a359-781a96219d2c)


Let's combine the `winner_id` and `loser_id` columns into a single column called `player_id`. Then, the main query selects the `player_id` column from the subquery and uses the `COUNT(*)` function to calculate the number of `games played` by each player.

```SQL
SELECT player_id, COUNT(*) AS games_played
FROM (
  SELECT winner_id AS player_id
  FROM your_table_name
  UNION ALL
  SELECT loser_id AS player_id
  FROM your_table_name
) AS subquery
GROUP BY player_id
```

![image](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/cc1d06e0-794c-4604-97db-39734f08073b)


Then, let's calculate the number of games `won`, and games `lost` for each player.

```SQL
WITH all_players AS (
  SELECT winner_id AS player_id, 'won' AS Result
  FROM your_table_name
  UNION ALL
  SELECT loser_id AS player_id, 'lost' AS Result
  FROM your_table_name
)
SELECT player_id, 
       COUNT(*) AS games_played,
       COUNT(CASE WHEN Result = 'won' THEN 1 END) AS won,
       COUNT(CASE WHEN Result = 'lost' THEN 1 END) AS lost
FROM all_players
GROUP BY player_id;
```

![image](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/697e0ed8-ae1f-441a-8165-2fbb42e8a36e)

We will calculate the win percentage by dividing the count of games won by the total count of games, multiplying it by 100, rounding it to two decimal places, and assigns it to the column alias `win_percentage`.

```SQL
WITH all_players AS (
  SELECT winner_id AS player_id, 'won' AS Result
  FROM your_table_name
  UNION ALL
  SELECT loser_id AS player_id, 'lost' AS Result
  FROM your_table_name
)
SELECT player_id, 
       COUNT(*) AS games_played,
       COUNT(CASE WHEN Result = 'won' THEN 1 END) AS won,
       COUNT(CASE WHEN Result = 'lost' THEN 1 END) AS lost,
       ROUND((COUNT(CASE WHEN Result = 'won' THEN 1 END) / COUNT(*)) * 100, 2) AS win_percentage
FROM all_players
GROUP BY player_id;
```

![image](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/1a050f03-82d0-4212-98f4-f42b2f8e9261)

Here, the `player_rank` column added, showing the rank of each player based on win percentage, along with other player statistics.

```SQL
WITH all_players AS (
  SELECT winner_id AS player_id, 'won' AS Result
  FROM your_table_name
  UNION ALL
  SELECT loser_id AS player_id, 'lost' AS Result
  FROM your_table_name
), player_stats AS (
  SELECT player_id, 
         COUNT(*) AS games_played,
         COUNT(CASE WHEN Result = 'won' THEN 1 END) AS won,
         COUNT(CASE WHEN Result = 'lost' THEN 1 END) AS lost,
         ROUND((COUNT(CASE WHEN Result = 'won' THEN 1 END) / COUNT(*)) * 100, 2) AS win_percentage
  FROM all_players
  GROUP BY player_id
)
SELECT RANK() OVER (ORDER BY win_percentage DESC) AS player_rank,
       player_id,
       games_played,
       won,
       lost,
       win_percentage
FROM player_stats
ORDER BY player_rank;
```

![image](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/af20de7f-fd41-4835-bff1-9dba818a858a)


Just as example, you will get different results with `ROW_NUMBER()` function:
```SQL
<...>
SELECT ROW_NUMBER() OVER (ORDER BY win_percentage DESC) AS player_rank,
       player_id,
       games_played,
       won,
       lost,
       win_percentage
FROM player_stats;
```

![image](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/f02786dd-3a95-4dff-951f-34da86f2466c)

Now, let's create a new table `connect_four_performance_summary`.

```SQL
CREATE TABLE connect_four.connect_four_performance_summary AS (
WITH all_players AS (
  SELECT winner_id AS player_id, 'won' AS Result
  FROM your_table_name
  UNION ALL
  SELECT loser_id AS player_id, 'lost' AS Result
  FROM your_table_name
), player_stats AS (
  SELECT player_id,
         COUNT(*) AS games_played,
         COUNT(CASE WHEN Result = 'won' THEN 1 END) AS won,
         COUNT(CASE WHEN Result = 'lost' THEN 1 END) AS lost,
         ROUND((COUNT(CASE WHEN Result = 'won' THEN 1 END) / COUNT(*)) * 100, 2) AS win_percentage
  FROM all_players
  GROUP BY player_id
)
SELECT RANK() OVER (ORDER BY win_percentage DESC) AS player_rank,
       player_id,
       games_played,
       won,
       lost,
       win_percentage
FROM player_stats
ORDER BY player_rank
);
```

![image](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/c1ca3bcd-fc4c-4bee-83aa-e45b9425d866)



Great job! With the `connect_four_performance_summary`, we now have all the information about the players' performance in a Connect Four game.


# Looker

In BigQuery export your data to the Looker Studio.

![image](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/34b54fda-088e-4329-bf52-4709758b25c3)


Create a dashboard. Here is my example.

![20240128_200142](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/e3956434-cf9f-4873-ba36-4b58aafca133)

# Send the results via email

Click on Share in Looker and Schedule delivery. You can filter the results (use Filters) and send them to each player based on the player_id.

![image](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/1074c957-ad5e-4bb8-8dc0-a3c024b4a163)

![image](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/9c14dbff-436e-44a7-8372-7456971a3082)


The participats will receive the PDF version of the report and the link to the interactive report.

![image](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/6f4be8e0-6f1f-4bd6-a0ba-2c09630ff434)



# Send the results to the Slack

Go to the https://api.slack.com

Create a new App.

![image](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/dcf054e2-c507-4e02-bc8b-4dcb83f06f28)


Open OAuth & Permissions and click on 'Add an OAuth Scope'.

Add these permissions

```
channels:read
channels:join
users:read
files:write
groups:read
im:read
mpim:read
chat:write
```
Copy Bot User OAuth Token: xoxb-<…>

Connect your App to the workspace. You will see your App in the Slack.

![image](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/50e025be-6001-469c-a28f-64ffbf5c979c)


Create a channel, e.g. game-results. Run the code to get the channel ID, you will see a similar output: Channel ID: CXXXXXX

```python
from slack_sdk import WebClient

#Create a client
token = "xoxb-<...>"
slack_client = WebClient(token=token)

#Get list of channels
channels = slack_client.conversations_list()
channel_id = None

#Find the channel ID based on the channel name
for channel in channels['channels']:
    if channel['name'] == 'game-results':
        channel_id = channel['id']
        break

if channel_id:
    print("Channel ID: ", channel_id)
else:
    print("Channel not found.")
```

Adjust this code to your needs: add `channel_id`, token, link to your csv file, `initial_comment`

```python
import requests
from slack_sdk import WebClient

#Create a client
token = "xoxb-<...>"
slack_client = WebClient(token=token)

#Join the channel
channel_id = "<...>"  #Replace with the actual channel ID
slack_client.conversations_join(channel=channel_id)

#Download the file from Google Cloud Storage
file_url = "https://storage.googleapis.com/your-bucket/connect-four-summary.csv"
response = requests.get(file_url)
file_contents = response.content

#Send a message and file
slack_client.files_upload(
    file=file_contents,
    channels=[channel_id],
    title='Connect Four Performance Summary',
    initial_comment='Thank you for your participation and enthusiasm throughout the games. Enjoy reviewing your performance and congratulations on your achievements!'
)
```

Run the code and you will see the message in the channel, you can also download the CSV file with the results.

![image](https://github.com/janaom/gcp-de-project-connect-four-with-python-dataflow/assets/83917694/f2eb78ee-891e-4da8-805d-18cc3bf2d510)

Congratulations 👏 
