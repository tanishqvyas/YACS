# Yet Another Centralized Scheduler



## Folder Structure
```
+
| --------- README.md
| --------- data
|	| --------- config.json
|	| --------- requests.py
| 
| --------- worker.py
| --------- master.py
| --------- analysis.py
|
| --------- Plots
| --------- project_information
|	| --------- Copy of BD_YACS_doc.docx
|	| --------- Copy of BD_YACS_presentation.pptx
| --------- Report
+
```

## Setup Instructions
1. Clone the repository by typing the following command:
``` git clone "https://github.com/tanishqvyas/YACS.git" ```

2. Navigate to the project directory by typing:
``` cd YACS ```

## Execution Instructions
1. Open 5 terminals. On the first 3 terminals type the following commands to run 3 worker nodes respectively:
``` python worker.py 4000 1 RR ```
``` python worker.py 4001 2 RR ```
``` python worker.py 4002 3 RR ```
The command line arguments passed are based on the configurations provided in **data/config.json**.
The 3rd command line argument is the scheduling algorithm name which can be **RR, LL or Random**.
This is passed only for the sake of handling the naming schemes for worker log files for different scheduling algorithm analysis.

2. In the 4th terminal, type the command to run the master node:
``` python master.py data/config.json RR ```
The command line arguments are the **PATH_TO_CONFIGURATION_FILE** and the scheduling algorithm which can be **RR, LL or Random**.

3. In the 5th terminal, run the command to send job requests to the master node for allocation to workers:
``` python data/requests.py 25 ```
where 25 is the number of requests to be sent.

4. Repeat steps 1-3 for other scheduling algorithms.

5. Run analysis.py to obtain the comparisons between the different scheduling algorithms.
```python analysis.py ```

