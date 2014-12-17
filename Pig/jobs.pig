--Register the UDFs
register 'pig/stemming.jar'; 
register 'pig/LevenshteinD.jar';

--Load file and preprocess the text
job = load 'jobtest.csv' using PigStorage(',') as (key:chararray,text:chararray); 
rawwords = foreach job generate key, flatten(TOKENIZE(text)) as word;
words = foreach rawwords generate key, LOWER(word) as word;

--Remove stop words
stopwords = load 'stopwords.txt' using TextLoader() as (stopword:chararray);
words_all = JOIN words BY word LEFT OUTER, stopwords BY stopword USING 'replicated'; 
nostop = filter words_all by stopword is null;
nostop_cleaned = foreach nostop generate key,word;

--Stemming on words 
nostem = foreach nostop generate key,stemming(word) as word;

--Correct misspelling of words
dictionary = load 'dictionary.txt' using TextLoader() as (dict:chararray);
dicted = JOIN nostem BY word LEFT OUTER, dictionary BY dict; 

rightw = filter dicted by $2 is not null; 
wrongw = filter dicted by $2 is null; 

wrong = foreach wrongw generate $0, $1, SUBSTRING($1,0,1) as first; 
dict_first = foreach dictionary generate dict, SUBSTRING(dict,0,1) as first; 

allwords = JOIN wrong BY first, dict_first by first USING 'replicated';
distances = foreach allwords generate $0 as key, $1 as mis, $3 as correct, LevenshteinD($1,$3) as dist; 
distances_grouped = group distances by key..mis; 
correcSpelling = FOREACH distances_grouped {
levdorder = ORDER distances by dist ASC;
top = LIMIT levdorder 1;
GENERATE FLATTEN(group), FLATTEN(top.correct) as corrected, FLATTEN(top.dist);
}

WrongFinal = foreach correcSpelling generate key, corrected;
RightFinal= FOREACH rightw GENERATE $0 AS key, $2 AS corrected;

jobdescription = UNION WrongFinal, RightFinal;
jobdesc_grouped = GROUP jobdescription BY key;
jobtemp = FOREACH jobdesc_grouped GENERATE $0 AS id, $1 AS word;
jobdescfinal = FOREACH jobtemp GENERATE id, word.$1;

STORE jobdescfinal INTO 'jobout' USING PigStorage();