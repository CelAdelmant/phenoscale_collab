# Phenoscale drone-derived tree phenology extraction

IN PROGRESS. This repository contains the code and data for the extraction and pre-processing of the drone-derived tree phenology and species composition data collected by the Sheldon Lab, University of Oxford. This repository is managed by
Celestine Adelmant, Irene Martinez-Baquero, Stephanie Koolen, and Jorgen Soraker. 

**Collaborators please create and work in personal branches off the *develop* branch**

## Contents

* Overview
* Defining buffers
* Defining Thiessen polygons
* Extracting NDVI
* Extracting tree composition

## This repo
This repository includes the following main features:

* An RStudio project phenoscale.Rproj
* Package dependency management using renv
* Configuration using config
* A scripts directory with the code necessary to reproduce the analysis and figures in this paper.
* An R folder for R source code and reusable functions

## Getting Started 
Clone this repository to your local computer using the following command in the terminal:
git clone https://github.com/celadelmant/phenoscale_collab.git

When adding new material (pre-made scripts, small datasets etc), add to *develop* branch. When working on editing shared scripts, please start a new branch based off the *develop* branch. Once the full task you are working on is complete, and you have checked that the code works (preferably with a check script), open pull request to merge this branch into *develop*. 

