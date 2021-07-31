# Collocation Extraction

In this assignment we automatically extract collocations from the Google 2-grams dataset using 
Amazon Elastic Map Reduce.

# Collocation

A collocation is a sequence of words or terms that co-occur more often than would be expected by 
chance. The identification of collocations - such as 'crystal clear', 'cosmetic surgery', 'איכות סביבה - 'is 
essential for many natural language processing and information extraction application.
In this assignment, we will use Normalized Pointwise Mutual Information (NPMI), in order to decide 
whether a given pair of ordered words is a collocation, where two ordered words with a high NPMI 
value are expected to be a collocation.

# Normalized PMI

Pointwise mutual information (PMI) is a measure of association used in information theory and 
statistics.
Given two words w1 and w2 and a corpus, we define normalized pmi as follows:



According to conditional probability definition and maximum likelihood estimation, the PMI can be 
expressed (think why) by:



Where c is count function and N is the total number of bi-grams in the corpus.
In order to adapt it for the task of identifying collocations of ordered pairs, we define c(w1,w2) as the 
count of the ordered pair w1w2 (excluding w2w1 pairs), c(w1) as the number of times w1 starts some 
bigram, c(w2) as the number of times w2 ends some bigram, and N as the number of bigrams.


# The Assignment

We provide a map-reduce job for collocation extraction for each decade, and run it in the 
Amazon Elastic MapReduce service. The collocation criteria will be based on the normalized PMI 
value:
- Minimal pmi: in case the normalized PMI is equal or greater than the given minimal PMI 
value (denoted by minPmi), the given pair of two ordered words is considered to be a 
collocation.
- Relative minimal pmi: in case the normalized PMI value, divided by the sum of all 
normalized pmi in the same decade (including those which their normalized PMI is less 
than minPmi), is equal or greater than the given relative minimal PMI value (denoted by 
relMinPmi), the given pair of two ordered words is considered to be a collocation.
Your map-reduce job will get the minPmi and the relMinPmi values as parameters.
You need to calculate the normalized pmi of each pair of words in each decade, and display all 
collations above each of the two minimum inputs. Run your experiments on the 2-grams Hebrew
corpus.
The input of the program is the Hebrew 2-Gram dataset of Google Books Ngrams.
The output of the program is a list of the collocations for each decade, and there npmi value, ordered 
by their npmi (descending).

# Stop Words
Stop words are words which are filtered out prior to, or after, processing of natural language data.
In this assignment, we remove all bigram that contain stop words and not include 
them in our counts.

# How To Run
The inputs for the assignment are the minPmi and relMinPmi mentioned above. Say that minPmi is 
0.5 and relMinPmi is 0.2, the command line to execute the assignment should look like:

_java -jar ass2.jar ExtractCollations 0.5 0.2_

#Scalability, Memory Assumptions
no assumptions were made on the size of the input. it can be as large as needed, and the application will run the same way.

# Outputs: 
https://s3.console.aws.amazon.com/s3/object/ass2jar?region=us-east-1&prefix=output4/AGAIN_yescombiner_partition_to_folder/part-r-00000

# Steps Explanation

##general explanation
Our program consists of four steps: count CW1 for all words in each decade, count CW2 for all words in each decade, 
calculate the NPMI and last  filter the collocations and sort the results. below there is the program flow scheme 
with examples inputs and outputs.


-------- first map reduce step: calculate CW1 for all words in each decade------------------------------------------------------
map input :   

word1	word2	1987	4
word1	word2	1981	3

word3	word4	1988	5
word5	word6	1989	6
word1	word2	1975	1
word3	word4	1976	2
word5	word6	1977	3
	.
	.
	.

mapper:
if not stop-word {
	write(<<1980, flag:1, word1> <word2,4> >)
    write(<<1980, flag:0, word1> <word2,4> >)
   }



shuffle and sort input:

<<1980,flag:1, word1> <word2,4>  >
<<1980,flag:0, word1> <word2,4>  >

<<1980, flag:1,word3> <word3,7> >
<<1980, flag:0,word3> <word3,7> >

<<1980, flag:1,word6> <word2,4> >
<<1980, flag:0,word6> <word2,4> >

<<1980, flag:1,word1> <word7,6> >
<<1980, flag:0,word1> <word7,6> >

             .
             .
             .
            
shuffle and sort: 

	first deacde
	then word
	then flag

reducer input : 
            
<<1980, flag:1, word1> [word2-4 word7-6 , word2-3...] >
<<1980, flag:0, word1> [word2-4 word7-6 , word2-3...] >
    .
    .
    .

combiner: 
sums up all the values for keys with flag=1, for flag=0 it is not possible.    
    
redcuer: 

if input.flag == 1{  
int cw1 = sum of input.values-iterator
}

> we can't go over input.values-iterator more than one time, so we send two copies of the key-value
>pair and use the flag to differentiate between them.  

for each value in input.values-iterator:
	write(<<1980, input.key.word, cw1, input.value.word >  input.value.count >    //example:  <<1980, word1,10 , word2> 4 >
								        //  <<1980, word1,10 , word7> 6 >


-------- second map reduce step: calculate CW2 for all words in each decade------------------------------------------------------
the second mapper (example)  input:

<<1980, word1, 10 > <word2, 4 >>
<<1980, word1,10> <word7,  6>>
<<1980, word1, 10 > <word2, 3> >

	^
	^
	^
 <<1980, word4-85> <word7, 6>>
 <<1980, word4-85> <word11,9>>


next mapper: 
	
 write(<<1980, flag:1, word2> ,<word1,CW1=10, count=4>>)
write(<<1980, flag:0,  word2> , <word1,CW1=10, count=4>>)

shuffle and sort: 

	first deacde
	then word2
	then flag
	
reducer input : 

	<<1980,flag:1, word2> ,[<word1,C1=10, count=4>,  <word8,CW1=15, count=3> ...] >
	<<1980,flag:0, word2> ,[<word1,C1=10, count=4>,  <word8,CW1=15, count=3> ...] >

combiner: 
sums up all the values for keys with flag=1, for flag=0 it is not possible.
    
reducer:

if input.flag == 1{  
int cw2 = sum of input.values-iterator
}

reducer output: 

<<1980, word1word2> count=3 , CW1 , CW2>
                .
                .
                .

<<1980, word3word2> count=11, CW3 , CW2>

------------------third  map-reduce-------------------calculate NPMI

map input :    

word1	word2	1980	4  24  56
word3	word4	1980	5   13  87   
word5	word6	1980	6   45  74

word1	word2	1970	1   12  18  
word3	word4	1970	2   68  31  
word5	word6	1970	3   2   87
	.
	.
	.

for each map input:

	write (<< 1980, word1word2, cw1=11, cw2=15> cw1w2=4  >)
	write(<<1980,#N#>1> )
	
> we use #N# to count all the words in the corpus
	

shuffle and sort: 

	first by decade,	
	
	then by twogram:
		first #N#, then word-word

reducer input:
	
	<<1970, #N#> [1,1,1...] >
		^
		^ 
		^ 

		^
		^
		^


	<<1970,word1word2 cw1=11, cw2=15>  [4 , 8 ,1...]   >
	<<1970,word1word4, cw1 =1 , cw4=11,> [11, 2, 17]>
	<<1970,word1word5 cw1=4 , cw5=7, >  [12 , 3, 5] >
	
		^
		^
		^
   
combiner: 
    the combiner simply sums #N# - the total number of 2-grams
    
reducer:   //has a private fields: N=0,  decade_NPMI = 0, curr_decade = 0, last_input;

if(input.key.twogram == #N#) {   
	this.N = sum(input.key.counts)
}

> the shuffle and sort assures that all #N# keys will get to the reducer first

if curr_decade != input.key.decade  {  			// start of new deacade
	if (curr_deacde !=0){       // NOT start of program
		write(<<last_input.key.decade, * *>decade_NPMI>)
	}


decade_NPMI = 0;
curr_decade = input.key.decade

}


C_w1w2 = input.getC1C2()
P_w1w2 = C_w1w2/N
C_w1 = input.getC1()
C_w2 = input,getC2()
PMI_w1w2 = LOG(C_w1w2) + LOG(N) - LOG(C_w1)-LOG(C_w2)

NPMI_w1w2 = PMI_w1w2/(-)LOG(P_w1w2)

write(<<input.key.decade, w1w2,> NPMI_w1w2 >)
decade_NPMI += NPMI_w1w2 
				

last_input= input
}


cleanup:  
> when we get to the end of the input we need to write one last time the decade_NPMI 
	
write(<<last_input.key.decade, * *>decade_NPMI>)

---------Last MAP-reduce----------- calculate collocation and sort


map input example:

1970 	w1	w2	0.8
1970 	w3	w4	0.1
1970 	w1	w3	0.3
		^
		^	
		^

1970    * * 	120.6	//decade_NPMI
	
map: 

for each input: 
	
		write(<<1970, 0.8, w1w2>, not-intersting>

>	for each decade we will do once :	write(<<1970, 120.6, * *> not-interesting >)

combiner:
the same as the reducer 

shuffle and sort:
	decade
	then * *
	then NPMI     
		

reducer input: 
	

	<<1970,* *,> [120.6]  >)
	
	<<1970,0.1, w1w2> not-intersting>

		^
		^
		^

	<<1970,0.5 w2w3>not-interesting>)	

reducer:  //has private field  decade_NPMI=0

if(input.key.second  == * * ) {
	decade_NPMI = input.val
}
else{
if (isCollocation(input, decade_NPMI, MIN_PMI, REL_PMI) ) {   // MIN_PMI and REL_PMI are inputs to main
		write(<<decade input.key> >)
}
}



# partitions

We divide the mapper outputs by tens digit
	







