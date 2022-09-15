#include <stdio.h>
#include <sys/wait.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#define N_gauss 1000 //
float *gauss(float ex, float dx, int n_point)
{
    time_t t;
    int i;
    float *mem1;
    mem1 = (float *)malloc(n_point * sizeof(float));
    srand((unsigned)time(&t));
    for (i = 0; i < n_point; i++)
        mem1[i] = (sqrt(-2 * log((float)rand() / 32768)) * cos((float)rand() / 32768 * 2 * 3.1415926)) * sqrt(dx) + ex;
    return (mem1);
}

int main(int argc, char **argv)
{
    FILE *dataset_file;
    char *dataset_filename = argv[1];
    char *queries_filename = argv[2];
    int queries_size = atoi(argv[3]);
    int ts_length = atoi(argv[4]);
    unsigned long int dataset_size = atoi(argv[5]);
    float noise_level = atof(argv[6]);
    float *ts_buffer = malloc(sizeof(float) * ts_length);
    FILE *queries_file = fopen(queries_filename, "wb");
    dataset_file = fopen(dataset_filename, "rb");
    time_t t;
    srand((unsigned)time(&t));
    unsigned long p;

    for (int i = 0; i < queries_size; i++)
    {

        p = (unsigned long)rand() % dataset_size;


        fseek(dataset_file, p * ts_length * sizeof(float), SEEK_SET);
        //printf("Query %d, Offset = %ld\n", (i + 1), p);
        fread(ts_buffer, sizeof(float), ts_length, dataset_file);

        for (int j = 0; j < ts_length; j++)
        {

            int a = rand() % 999 + 1;
            float f = (float)a;
            f = f / 1000.0f;
            float c = sqrt(-2.0 * log(f));
            int a2 = rand() % 1000;
            float f2 = (float)a2;
            f2 = f2 / 1000.0f;
            float b = 2 * 3.1415926 * f2;
            float noise = (float)c * cos(b) * sqrt(noise_level);
            // float nosie=(float)c*cos(b)*noiselevel;
            // printf("old value = %g", ts_buffer[j]);
            ts_buffer[j] = ts_buffer[j] + noise;
            // printf("           new value =%g\n", ts_buffer[j]);
        }
        float sum = 0.0f;
        float stdd = 0.0f;
        for (int j = 0; j < ts_length; j++)
        {
            sum = sum + ts_buffer[j];
        }
        float average = sum / ts_length;
        for (int j = 0; j < ts_length; j++)
        {
            stdd = stdd + (ts_buffer[j] - average) * (ts_buffer[j] - average);
        }
        float stdev = sqrt(stdd / ts_length);

        for (int j = 0; j < ts_length; j++)
        {
            ts_buffer[j] = (ts_buffer[j] - average) / stdev;
        }
        fwrite(ts_buffer, sizeof(float), ts_length, queries_file);
    }

    fclose(dataset_file);
    fclose(queries_file);
    free(ts_buffer);
    return 0;
}
