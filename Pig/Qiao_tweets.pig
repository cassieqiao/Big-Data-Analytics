--Load the tweets and preprocess the text
tweets = load 'tweets/tweets_*.txt' using PigStorage('|') as (date:chararray,key:chararray,name1,name2,name3,r1,r2,r3,r4,tweet:chararray); 
allwords = foreach tweets generate CONCAT(date,key) as key, flatten(TOKENIZE(tweet)) as word; 
words = foreach allwords generate key, LOWER(word) as word; 

--Load good and bad words and calculate sentiment
goodwordsraw = load 'tweets/good.txt' using TextLoader() as (good:chararray);
goodwords = foreach goodwordsraw generate good, 1 as value;
badwordsraw = load 'tweets/bad.txt' using TextLoader() as (bad:chararray); 
badwords = foreach badwordsraw generate bad, -1 as value;

words_good = JOIN words BY word, goodwords BY good using 'replicated'; 
words_bad = JOIN words BY word, badwords BY bad using 'replicated'; 

goodandbad = UNION words_good,words_bad;
goodandbad_grouped = GROUP goodandbad BY key;
groupedtweet = foreach goodandbad_grouped generate $0 as key, $1 as wordTuple;
tweetsentiment = foreach groupedtweet generate key, SUM(wordTuple.$3) as sentiment;

Postweet = filter tweetsentiment by sentiment > 0;
ALLpos = GROUP Postweet ALL;
Pos_count = foreach ALLpos generate COUNT(Postweet);

Negtweet = filter tweetsentiment by sentiment < 0;
ALLneg = GROUP Negtweet ALL;
Neg_count = foreach ALLneg generate COUNT(Negtweet);

--Store results
Store Pos_count INTO 'Pos' using PigStorage();
Store Neg_count INTO 'Neg' using PigStorage();


