#include <avro.h>
#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>

#ifdef DEFLATE_CODEC
#define QUICKSTOP_CODEC  "deflate"
#else
#define QUICKSTOP_CODEC  "null"
#endif

avro_schema_t dns_schema;
avro_schema_t stinger_schema;

/* Schema we are given for the DNS_cap data */
const char DNS_SCHEMA[] =
"{\"type\":\"record\",\
  \"name\":\"ActiveDns\",\
  \"namespace\":\"astrolavos.avro\",\
  \"avro.codec\": \"snappy\",\
  \"fields\":[\
        {\"name\":\"date\",\"type\":\"string\"},\
        {\"name\":\"qname\",\"type\":\"string\"},\
        {\"name\":\"qtype\",\"type\":\"int\"},\
        {\"name\":\"rdata\",\"type\":[\"string\",\"null\"]},\
        {\"name\":\"ttl\",\"type\":[\"int\",\"null\"]},\
        {\"name\":\"authority_ips\",\"type\":\"string\"},\
        {\"name\":\"count\",\"type\":\"long\"},\
        {\"name\":\"hours\",\"type\":\"int\"},\
        {\"name\":\"source\",\"type\":\"string\"},\
        {\"name\":\"sensor\",\"type\":\"string\"}]}";

const char STINGER_SCHEMA[] = 
"{\"type\":\"record\",\
  \"name\":\"ActiveDns\",\
  \"namespace\":\"astrolavos.avro\",\
  \"avro.codec\": \"snappy\",\
  \"fields\":[\
        {\"name\":\"qname\",\"type\":\"string\"},\
        {\"name\":\"qtype\",\"type\":\"int\"},\
        {\"name\":\"rdata\",\"type\":[\"string\",\"null\"]},\
        {\"name\":\"authority_ips\",\"type\":\"string\"},\
        {\"name\":\"count\",\"type\":\"long\"}]}";

//Dynamic array implementation for directory listing
typedef struct{
    char **array;
    size_t used;
    size_t size;
}Array;

void initArray(Array *a, size_t initialSize){
    a->array = (char **)malloc(initialSize*256);
    a->used = 0;
    a->size = initialSize;
}

void insertArray(Array *a, char *element){
  //fprintf(stdout, "Inserting: %s\n", element);
    if ((a->used * 256) == (a->size * 256)){
            a->size *=2;
            void *newPointer;
            if( (newPointer = ((char **)realloc(a->array, a->size * 256)))){
                a->array = newPointer;
            }
            else{
                fprintf(stderr, "REALLOC FAILED");
            }
    }
    a->array[a->used] = malloc(strlen(element));
    strcpy(a->array[a->used], element);
    a->used = a->used + 1;
}

void freeArray(Array *a){
    a->array = NULL;
    a->used = a->size = 0;
}

void printArray(Array *a){
    int k = 0;
    fprintf(stdout, "### PRINTING ARRAY ###\n");
    fprintf(stdout, "Array size: %zu\n", (a->size));
    float percent = ((double)a->used / a->size)* 100; 
    fprintf(stdout, "Percent Used: %f\n", percent);
    for(k = 0; k < (a->used); k++){
        fprintf(stdout, "Element %d: %s\n", k, a->array[k]);
    }
}

/* Parse schema into a schema data structure */
void init_schema(void)
{
        if (avro_schema_from_json_literal(DNS_SCHEMA, &dns_schema)) {
                fprintf(stderr, "Unable to parse DNS schema\n");
                exit(EXIT_FAILURE);
        }
}

int print_record(avro_file_reader_t db, avro_schema_t reader_schema)
{
        int rval;
        avro_value_t record;
        avro_value_iface_t *iface = avro_generic_class_from_schema(reader_schema);
        avro_generic_value_new(iface, &record);
        rval = avro_file_reader_read_value(db, &record);
        if (rval == 0) {
               int64_t i64;
               int32_t i32;
               const char *p;

               avro_value_t date_value,
                            qname_value, 
                            qtype_value,
                            rdata_value,
                            rdata_union_value,
                            ttl_value,
                            ttl_union_value,
                            authority_ips_value, 
                            count_value,
                            hours_value;

               size_t DATE          = 0;
               size_t QNAME         = 1;
               size_t QTYPE         = 2;
               size_t RDATA         = 3;
               size_t TTL           = 4;
               size_t AUTHORITY_IPS = 5;
               size_t COUNT         = 6;
               size_t HOURS         = 7;

               if(avro_value_get_by_index(&record, DATE, &date_value, NULL) == 0){
                   if(avro_value_get_string(&date_value, &p, NULL) == 0){
                    fprintf(stdout, "%s\t", p);
                   }
               }
               if(avro_value_get_by_index(&record, QNAME, &qname_value, NULL) == 0){
                   if(avro_value_get_string(&qname_value, &p, NULL) == 0){
                    fprintf(stdout, "%s\t", p);
                   }
               }

               if(avro_value_get_by_index(&record, QTYPE, &qtype_value, NULL) == 0){
                   if(avro_value_get_int(&qtype_value, &i32) == 0){
                    fprintf(stdout, "%d\t", i32);
                   }
               }
               //Since RDATA and TTL have two possible data types we set our code to
               //only try and print the type of index 0 (string for rdata, int for TTL)
               if(avro_value_get_by_index(&record, RDATA, &rdata_value, NULL) == 0){
                   if(avro_value_set_branch(&rdata_value, 0, &rdata_union_value) == 0){
                       if(avro_value_get_string(&rdata_union_value, &p, NULL) == 0){
                        fprintf(stdout, "%s\t", p);
                       }
                   }
               }
               if(avro_value_get_by_index(&record, TTL, &ttl_value, NULL) == 0){
                   if(avro_value_set_branch(&ttl_value, 0, &ttl_union_value) == 0){
                       if(avro_value_get_int(&ttl_union_value, &i32) == 0){
                        fprintf(stdout, "%d\t", i32);
                       }
                   }
               }
               if(avro_value_get_by_index(&record, AUTHORITY_IPS, &authority_ips_value, NULL) == 0){
                   if(avro_value_get_string(&authority_ips_value, &p, NULL) == 0){
                    fprintf(stdout, "%s\t", p);
                   }
               }
               if(avro_value_get_by_index(&record, COUNT, &count_value, NULL) == 0){
                   if(avro_value_get_long(&count_value, &i64) == 0){
                    fprintf(stdout, "%ld\t", i64);
                   }
               }
               if(avro_value_get_by_index(&record, HOURS, &hours_value, NULL) == 0){
                   if(avro_value_get_int(&hours_value, &i32) == 0){
                    fprintf(stdout, "%d", i32);
                   }
                }
               fprintf(stdout, "\n");
               //Reset the values to reduce the number of malloc/free calls we need to make for each iteration.
               //This allows us to use the same memory for each iteration.
               //create pointers to check and see if values are not null before attempting to release them.
               //This is useful for using projections so we dont check for unused values.
               avro_value_reset(&date_value);
               avro_value_reset(&qname_value);
               avro_value_reset(&qtype_value);
               avro_value_reset(&rdata_value);
               avro_value_reset(&rdata_union_value);
               avro_value_reset(&ttl_value);
               avro_value_reset(&ttl_union_value);
               avro_value_reset(&authority_ips_value);
               avro_value_reset(&count_value);
               avro_value_reset(&hours_value);

               //Give back memory we no longer need
               avro_value_decref(&record);
               avro_value_iface_decref(iface);
        }
        return rval;
}


int main( int argc, char *argv[] )
{
        avro_file_reader_t file_reader;
        int len;
        //Create struct pointer for directory entry
        struct dirent *pDirent;
        /*
         * Create struct for the directory stream you are opening.
         * Will include the following members:
         *      ino_t d_ino File serial number
         *      char d_name[] Name of entry.
        */
        DIR *pDir;

        if (argc == 2){
            pDir = opendir(argv[1]);
            if (pDir == NULL){
                fprintf(stderr, "Cannot open directory '%s'\n", argv[1]);
                exit(EXIT_FAILURE);
            }
        }
        else if (argc > 2) {
            fprintf(stderr, "Toomany arguments supplied\n");
            exit(EXIT_FAILURE);
        }
        else{
            fprintf(stdout, "Please provide a source file");
        }

        ///* Initialize the schema structure from JSON */
        init_schema();

        //Init dynamic array with a size of 10
        //TODO: Adjust the init value to be something optimized for what our use case is
        Array avro_files;
        initArray(&avro_files, 10);
        Array *aPtr = &avro_files;

        // For each file in the directory check if it ends in ".avro"
        // if it does add it to an array
        while ((pDirent = readdir(pDir)) != NULL){
            len = strlen(pDirent->d_name);
            if( (len >= 5) && strcmp(&(pDirent->d_name[len-5]), ".avro") == 0){
                insertArray(&avro_files, pDirent->d_name);
            }
        }
        // For each avro file in the directory we instantiate a file reader
        // then pass the file handle and dns scheme to our print record function
        int i;
        char file_name[256];
        int print_ret_val;
        for(i =0; i < avro_files.used; i++){
            //Preprend path to file name
            if(snprintf(file_name, sizeof(file_name), "%s%s", argv[1], avro_files.array[i]) < 0){
                fprintf(stderr, "Remove trailing slash from directory path.");
                break;
                //return EXIT_FAILURE;
            }
            ///* Read all the records and print them */
            if (avro_file_reader(file_name, &file_reader) == 0) {
                do{
                    print_ret_val = print_record(file_reader, dns_schema);
                    //If end of file print a status update and move to next file
                    if (print_ret_val == EOF) {
                        fprintf(stderr, "Finished reading file (%d/%zu): %s\n", i+1, avro_files.used, avro_files.array[i]);
                        //avro_file_reader_close(file_reader);
                        break;
                    }
                    //If there is an error report the file it came from and move to next file
                    else if(print_ret_val != 0){
                        fprintf(stderr, "Error reading from %s\n", avro_files.array[i]);
                        break;
                    }
                }while( 1 );
            }
            else{
                fprintf(stderr, "Error opening file: %s\n", avro_strerror());
            }
        }
        avro_file_reader_close(file_reader);
        freeArray(aPtr);
        avro_schema_decref(dns_schema);

        return 0;
}
