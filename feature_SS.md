#Feature Subset Strategy
We compared different Feature Subset Strategies for the Random Forest Classifier found in MLlib for Spark for Star-Galaxy classification of the cfhtlens.csv data.

MLlib only allows input of strings: "auto", "sqrt", "log2", "all", and "onethird".
In our case, "sqrt" is the same as "auto".

These metrics were averaged over 5 trials each.
![Alt text](images/10x4.png)
![Alt text](images/10x10.png)
![Alt text](images/50x4.png)
![Alt text](images/50x10.png)