//
//  main.c
//  tsgen
//
//  Created by Kostas Zoumpatianos on 3/27/12.
//  Copyright 2012 University of Trento. All rights reserved.
//
#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <math.h>
#include <readline/readline.h>
#include <getopt.h>
#include <string.h>
#include <gsl/gsl_rng.h>
#include <gsl/gsl_randist.h>
#define PRODUCT "TSutils - Time Series Generator\n\
Copyright (C) 2012 University of Trento\n\n"
#define STD 1   // Standard deviation 

void z_normalize(float *ts, int size);

void inline z_normalize(float *ts, int size) {
        int i;
        float mean = 0;//gsl_stats_mean(ts, 1, size);
        float std = 0;//gsl_stats_sd(ts, 1, size);
        for (i=0; i<size; i++)
        {
            mean += ts[i];
        }
        mean /= size;

        for (i=0; i<size; i++) 
        {
            std += (ts[i] - mean) * (ts[i] - mean); 
        }
        std /= size;
            std = sqrt(std); 
        for (i = 0; i < size; i++)
        { 
            ts[i] = (ts[i] - mean) / std;
        }   
}

float * generate (float *ts, int size, gsl_rng * r, char normalize) {
    int i;
    float x = 0, dx;

    for (i = 0; i < size; i++)
    {
        dx = gsl_ran_gaussian (r, STD); // mean=0, std=STD
        x += dx;
        ts[i] = x;
    }

    if(normalize == 1)
    {
        z_normalize(ts, size);
    }
    return ts;
}

/**
    Parses the command line arguments.
**/
void parse_args (int argc, char **argv, int *length, int *number_of_timeseries,
                float *skew_frequency, char *normalize, long int *labelsize) {
    while (1)
    {
        static struct option long_options[] =  {
            {"skew-frequency", required_argument, 0, 'f'},
            {"length", required_argument, 0, 'l'},
            {"size", required_argument, 0, 's'},
            {"z-normalize", no_argument, 0, 'z'},
            {"label-size", required_argument, 0, 'k'},
            {"help", no_argument, 0, 'h'},
            {NULL, 0, NULL, 0}
        };

        /* getopt_long stores the option index here. */
        int option_index = 0;

        int c = getopt_long (argc, argv, "",
                             long_options, &option_index);
        if (c == -1)
            break;
        switch (c)
        {
        case 'f':
            *skew_frequency = atof(optarg);
            break;
        case 's':
            *number_of_timeseries = atoi(optarg);
            break;
        case 'l':
            *length = atoi(optarg);
            break;
        case 'k':
            *labelsize = atoi(optarg);
            break;
        case 'z':
            *normalize = 1;
            break;
        case 'h':
            printf(PRODUCT);
            printf("Usage:\n\
                       \t--size XX \t\tThe number of time series to generate\n\
                       \t--length XX \t\tThe length of each time series\n\
                       \t--skew-frequency XX \tThe skewness frequency\n\
                       \t--z-normalize \t\tUse to enable z-normalization\n\
                       \t--help\n\n");
            exit(-1);
            break;
        default:
            exit(-1);
            break;
        }
    }
}

/**
    Generates a set of random time series.
**/
void generate_random_timeseries(int length, int number_of_timeseries,
                                char normalize, int repetition) 
{
    // Initialize random number generation
    const gsl_rng_type * T;
    gsl_rng * r;
    gsl_rng_env_setup();
    T = gsl_rng_default;
    r = gsl_rng_alloc (T);

    float *ts = malloc(sizeof(float) * length);
    int i, j, rep;
    for (i=1; i<=number_of_timeseries; i+=repetition)
    {
        generate(ts, length, r, normalize);

        for(rep=0; rep<repetition; rep++)
        {
	  
	  fwrite(ts, sizeof(float), length, stdout);
          /*   for(j=0; j<length; j++) {
                 printf ("%g ", ts[j]);
             }
             printf("\n");	 
	  */      
        }

        if(i % (1000 * repetition) == 0) {
            fprintf(stderr,"\r\x1b[m>> Generating: \x1b[36m%2.2lf%%\x1b[0m",(float) ((float)i/(float)number_of_timeseries) * 100);
        }
    }
    fprintf(stderr, "\n");


    // Finalize random number generator
    gsl_rng_free (r);
}
void generate_random_label( int number_of_timeseries, int repetition, long int labelsize) 
{
    // Initialize random number generation
    srand(time(NULL));   // should only be called once
    int r = rand(); 
    long int *label = malloc(sizeof(long int) * 1);
    int i, j, rep;
    for (i=1; i<=number_of_timeseries; i+=repetition)
    {

        for(rep=0; rep<repetition; rep++)
        {
        *label=(long int)(rand()%(labelsize-1));
        fwrite(label, sizeof(long int), 1, stdout);
          /*   for(j=0; j<length; j++) {
                 printf ("%g ", ts[j]);
             }
             printf("\n");   
      */      
        }
        if(i % (1000 * repetition) == 0) {
            fprintf(stderr,"\r\x1b[m>> Generating: \x1b[36m%2.2lf%%\x1b[0m",(float) ((float)i/(float)number_of_timeseries) * 100);
        }
    }
    fprintf(stderr, "\n");


    // Finalize random number generator
}


int main(int argc, char **argv) {

    // Initialize variables
    int length = 0;                 // Length of a single time series
    int number_of_timeseries = 0;   // Number of time series to generate

    float skew_frequency = 0;           // The skew frequency
    int repetition = 1;             // How many times each time series is repeated
    char normalize = 0;             // Normalize or not.
    long int labelsize=0;           // label size
    // Parse command line arguments
    parse_args(argc, argv, &length, &number_of_timeseries, &skew_frequency, &normalize, &labelsize);

    fprintf(stderr,PRODUCT);

    if((1-skew_frequency) > 0)
        repetition = number_of_timeseries / (number_of_timeseries * (1-skew_frequency));
    else
        repetition = number_of_timeseries;

    fprintf(stderr, ">> Generating random time series...\n");
    if (labelsize==0)
    {
            generate_random_timeseries(length, number_of_timeseries, normalize, repetition);
    }
    else
    {
            generate_random_label(number_of_timeseries, repetition, labelsize);
    }

    fprintf(stderr, ">> Done.\n");
    

    return 0;
}
