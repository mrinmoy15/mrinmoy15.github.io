###Question no1 Answer:
Name<-c("Ramya","Ali","Jim")
Age<-c(25,30,35)
Telephone_bill_rs<-c(600,400,200)
Month<-c("Aug","Aug","Aug")
customer_details<-data.frame(Name,Age,Telephone_bill_rs,row.names = c("row1","row2","row3"))
customer_details

###Question no 2 Answer:
## part a
Names<-list(LastName=c("Potter","Riddle","Dumbledore"),FirstName=c("Hary","Tom","Albus"),
            Age=c(18,50,120),Profession=c("Student","Magician","Headmaster"))

## part b
Names$LastName
## or 
Names[[1]]

##Part c
Names$Age[3]
##or
Names[[3]][3]

## Question no 3 answer:
library(ggplot2)
?msleep
library(datasets)
data("msleep")
msleep$name
class(msleep)
library(data.table)
setnames(msleep,old="vore",new = "type")
names(msleep)
head(msleep$type,10)
