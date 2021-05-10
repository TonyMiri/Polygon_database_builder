This was a project to build a PostgreSQL database of historical stock data pulled from the Polygon.io stock data API.
 
- If you'd like to use it you'll need a subscription to the Polygon.io service.
- Be sure to replace 'YOUR_API_KEY' in the polygon_functions_and_keys file with your actual Polygon API key.
- Naturally you'll need Postgres installed.
- The file db_tickers is a list of all the stocks that ended up in the database. If you are to run the script and are interested in American stocks then that is a list of all the American stocks that Polygon.io currently has price data for and should be used as the list in the build_database() function. This file was also used by me to create a main table containing all tickers after I created all the price tables. That functionality is found in the DB_modifications file. (I TOLD YOU I DIDN'T THINK IT THROUGH)
- The various text files included were used throughout the building of the scripts and most are not integral to the functioning of the script, but are included as reference material in case you want to make any modifications to the functionality based on the data. 
- The tickers NAKD and SNDL were not included due to variations in the data causing errors. I skipped them as they are penny stocks anyway (as of May 2021) and won't be using them personally.
- It didn't run completely cleanly through the entire list due to unforseen and aformentioned variations in the data. I was using us_tickers_list as my list of tickers to build the database (which you shouldn't use because it contains A LOT of tickers that don't have price data. Use db_tickers instead). I made a copy of us_tickers_list at the beginning and named it us_tickers_list2, and every time the program hung up and stopped on a certain ticker I just deleted that ticker and all tickers before it from us_tickers_list2 and ran the program from that point. Not ideal, obviously, but it worked fairly cleanly.
- There is a repo from another guy on here that uses Pandas and you should probably use that one if you just want daily data. I did it without Pandas because I'm a glutton for punishment.
- It's not done very cleanly because I started the project without a clearly defined scope. I aimed too big at first then ended up getting more specific with functions later.
- This was my first time building a database.
