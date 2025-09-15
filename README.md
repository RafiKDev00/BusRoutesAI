
B''SD
# AI Bus Routes

There's a lot of terminology to assimilate and understand. My "layperson's" description is: I want to expose an AI to my postgres database and then ask inferential questions based on my data, without any of my data going outside my environment.

So after "showing the AI" my database which has a table of routes, where each route has n stops, going from simple to complex:
  * Which route is the shortest?
  * Which route has the longest distance between 2 stops?
  * Is Account X's routes "better" or "worse" than Account Y's routes, if we define route quality as average distance between stops?
and so on... and that's just "route questions". I have dozens of others for other areas of our database.

Look for sample projects that use Bedrock based on a postgres db, see if I can get it to answer questions like this as a PoC...Let's make some of our own sample data.

---

## File structure

|File/Folder      | Purpose              | 
|-----------------|----------------------|
|`load_gfts_to_rds.py`| Takes sample data from Washington Metro and Uploads it to AWS RDS|
|`anaylsis_queries.py`| SQL again written by GPT...but that's not why we're here...Explanations and validation as to WHY these queries work enclosed though (written by yours truly)|
|`gfts_data`      | a folder of GTFS files downladed from Washington Metro websiter, These are not on GITHUB, you, user, will need to get then yourself, API rules|
|`inspect_schema.py`| A quick script to check our database headings|
|`sql_explanation_and_output.txt`|
|`main.py`        | Our driver - still empty...might be useful for bedrock      |

---
## General Notes

This is a sample PoC project. Not intended for any financial, technical, legal, or practical use beyond the theoretical. We are here to prove the use of Amazon Bedrock, for certain exploratory purposes. That is all.

source .venv/bin/activate