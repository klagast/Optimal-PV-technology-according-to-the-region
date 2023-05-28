# Optimal-PV-technology-according-to-the-region
## Introduction
Hello everyone,
this is the code for the survey "Optimal PV technology according to the region".
The aim of the study was to use pvlib PV to do calculations with the Sandia Module Database for cities around the world. 
After applying data analysis, the most "optimal" PV technolgy in each self-defined region was determined. 
The results were visualised in a Dash Python application.
## Structure
This project consists of three parts each with their own readme file:
- The calculations folder: The calculations to calculate the energy yield for the cities with the Sandia Array Performance Model are described here.
- The analysis folder: The analysis of the results in this thesis with heatmaps, histograms, boxplots, etc.
- The Dash app folder: The Dash application in Python where an interactive world map is visualised of about 4000 cities. When you click on a city, you should go to a analysis page, where a suggestion is given of what is the most optimal PV technology in the chosen region. 

## Disclaimers
- This research was done by a student and was never checked for correctness or accuracy of the results.
- This research was conducted by a student who had limited basic knowledge of Python at the start. So some pieces of code could definitely be coded more efficiently.
- This research was combined with data stored locally in a PostgreSQL database.
- Understanding the code without the written research text is probably not possible.
