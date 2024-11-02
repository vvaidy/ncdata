#!/bin/sh
BUCKET_URL="https://s3.amazonaws.com"
ABS_BUCKET_NAME="dl.ncsbe.gov/ENRS/2024_11_05"
VOTER_DATA_BUCKET_NAME="dl.ncsbe.gov/data"

DO_SQLITE="no" # "yes" or "no" to run the sqlite3 import

# Database Variables
DATABASE="ncdata.db"

# Runtime Variables
DATE=$(date +%Y%m%d)
UNZIP=unzip
WGET=wget
#FOR DEV
# UNZIP="echo unzip"
# WGET="echo wget"
# DATE="20241020"

FILE_LIST="absentee_20241105.zip \
            absentee_counts_county_20241105.csv \
            absentee_counts_state_20241105.csv \
            absentee_county_20241105.zip \
            absentee_demo_stats_20241105.csv \
            absentee_spoiled_outstanding_20241105.zip \
            absentee_spoiled_outstanding_20241105_20241105.csv \
            absentee_spoiled_outstanding_20241105_20241106.csv \
            polling_place_20241105.csv \
            provisional_20241105.txt \
            results_pct_20241105.zip"

if [ -d $DATE ]; then
    echo "Directory $DATE already exists. Skipping download step."
else
    mkdir -p $DATE
    cd $DATE
    
    #### Download absentee/early voting files
    for FILE in $FILE_LIST
    do
        $WGET "$BUCKET_URL/$ABS_BUCKET_NAME/$FILE"
    done
    # Unzip absentee file, we just keep the rest around for future reference
    $UNZIP absentee_20241105.zip
    # Download and unzip Voter History files
    wget "$BUCKET_URL/$VOTER_DATA_BUCKET_NAME/ncvhis_Statewide.zip"
    $UNZIP ncvhis_Statewide.zip
    # Download and unzip Voter Registration Data files
    wget "$BUCKET_URL/$VOTER_DATA_BUCKET_NAME/ncvoter_Statewide.zip"
    $UNZIP ncvoter_Statewide.zip

    cd .. # done with data for this date
fi

if [ "$DO_SQLITE" = "yes" ]; then
    # SQLITE3 Database import

    if [ -f "$DATABASE" ]; then
        echo "Saving existing $DATABASE to PRE/$DATABASE.PRE.$DATE"
        mv "$DATABASE" "PRE/$DATABASE.PRE.$DATE"
    fi

    # create the tables so we can import the data
    sqlite3 "$DATABASE" <<EOF
.echo on

CREATE TABLE absentee_counts (  
    county_desc                     CHAR(20),  
    voter_reg_num                   CHAR(12),  
    ncid                            CHAR(12),  
    voter_last_name                 CHAR(30),  
    voter_first_name                CHAR(60),  
    voter_middle_name               CHAR(20),  
    race                            CHAR(60),  
    ethnicity                       CHAR(3),  
    gender                          CHAR(60),  
    age                             INTEGER,  
    voter_street_address            CHAR(75),  
    voter_city                      CHAR(60),  
    voter_state                     CHAR(2),  
    voter_zip                       CHAR(9),  
    ballot_mail_street_address      CHAR(75),  
    ballot_mail_city                CHAR(60),  
    ballot_mail_state               CHAR(2),  
    ballot_mail_zip                 CHAR(9),  
    other_mail_addr1                CHAR(75),  
    other_mail_addr2                CHAR(75),  
    other_city_state_zip            CHAR(60),  
    relative_request_name           CHAR(75),  
    relative_request_address        CHAR(75),  
    relative_request_city           CHAR(60),  
    relative_request_state          CHAR(2),  
    relative_request_zip            CHAR(9),  
    election_dt                     DATE,  
    voter_party_code                CHAR(3),  
    precinct_desc                   CHAR(30),  
    cong_dist_desc                  CHAR(30),  
    nc_house_desc                   CHAR(30),  
    nc_senate_desc                  CHAR(30),  
    ballot_req_delivery_type        CHAR(30),  
    ballot_req_type                 CHAR(30),  
    ballot_request_party            CHAR(3),  
    ballot_req_dt                   DATE,  
    ballot_send_dt                  DATE,  
    ballot_rtn_dt                   DATE,  
    ballot_rtn_status               CHAR(30),  
    site_name                       CHAR(100),  
    sdr                             CHAR(3),  
    mail_veri_status                CHAR(20)  
);

CREATE TABLE voter_history (  
    county_id               INTEGER,  
    county_desc             VARCHAR(20),  
    voter_reg_num           CHAR(12),  
    election_lbl            CHAR(10),  
    election_desc           VARCHAR(230),  
    voting_method           VARCHAR(10),  
    voted_party_cd          VARCHAR(3),  
    voted_party_desc        VARCHAR(60),  
    pct_label               VARCHAR(6),  
    pct_description         VARCHAR(60),  
    ncid                    VARCHAR(12),  
    voted_county_id         INTEGER,  
    voted_county_desc       VARCHAR(60),  
    vtd_label               VARCHAR(6),  
    vtd_description         VARCHAR(60)  
);

CREATE TABLE voter_registration (  
    county_id                   INTEGER,  
    county_desc                 VARCHAR(15),  
    voter_reg_num               CHAR(12),  
    ncid                        CHAR(12),  
    last_name                   VARCHAR(25),  
    first_name                  VARCHAR(20),  
    middle_name                 VARCHAR(20),  
    name_suffix_lbl             CHAR(3),  
    status_cd                   CHAR(2),  
    voter_status_desc           VARCHAR(25),  
    reason_cd                   VARCHAR(2),  
    voter_status_reason_desc    VARCHAR(60),  
    res_street_address          VARCHAR(65),  
    res_city_desc               VARCHAR(60),  
    state_cd                    VARCHAR(2),  
    zip_code                    CHAR(9),  
    mail_addr1                  VARCHAR(40),  
    mail_addr2                  VARCHAR(40),  
    mail_addr3                  VARCHAR(40),  
    mail_addr4                  VARCHAR(40),  
    mail_city                   VARCHAR(30),  
    mail_state                  VARCHAR(2),  
    mail_zipcode                CHAR(9),  
    full_phone_number           VARCHAR(12),  
    confidential_ind            CHAR(1),  
    registr_dt                  DATE,  
    race_code                   CHAR(3),  
    ethnic_code                 CHAR(3),  
    party_cd                    CHAR(3),  
    gender_code                 CHAR(1),  
    birth_year                  CHAR(4),  
    age_at_year_end             CHAR(3),  
    birth_state                 VARCHAR(2),  
    drivers_lic                 CHAR(1),  
    precinct_abbrv              VARCHAR(6),  
    precinct_desc               VARCHAR(60),  
    municipality_abbrv          VARCHAR(6),  
    municipality_desc           VARCHAR(60),  
    ward_abbrv                  VARCHAR(6),  
    ward_desc                   VARCHAR(60),  
    cong_dist_abbrv             VARCHAR(6),  
    super_court_abbrv           VARCHAR(6),  
    judic_dist_abbrv            VARCHAR(6),  
    nc_senate_abbrv             VARCHAR(6),  
    nc_house_abbrv              VARCHAR(6),  
    county_commiss_abbrv        VARCHAR(6),  
    county_commiss_desc         VARCHAR(60),  
    township_abbrv              VARCHAR(6),  
    township_desc               VARCHAR(60),  
    school_dist_abbrv           VARCHAR(6),  
    school_dist_desc            VARCHAR(60),  
    fire_dist_abbrv             VARCHAR(6),  
    fire_dist_desc              VARCHAR(60),  
    water_dist_abbrv            VARCHAR(6),  
    water_dist_desc             VARCHAR(60),  
    sewer_dist_abbrv            VARCHAR(6),  
    sewer_dist_desc             VARCHAR(60),  
    sanit_dist_abbrv            VARCHAR(6),  
    sanit_dist_desc             VARCHAR(60),  
    rescue_dist_abbrv           VARCHAR(6),  
    rescue_dist_desc            VARCHAR(60),  
    munic_dist_abbrv            VARCHAR(6),  
    munic_dist_desc             VARCHAR(60),  
    dist_1_abbrv                VARCHAR(6),  
    dist_1_desc                 VARCHAR(60),  
    vtd_abbrv                   VARCHAR(6),  
    vtd_desc                    VARCHAR(60)  
);

.mode csv
.import --skip 1 "$DATE/absentee_20241105.csv" absentee_counts
.mode tabs
.import --skip 1 "$DATE/ncvhis_Statewide.txt" voter_history
.import --skip 1 "$DATE/ncvoter_Statewide.txt" voter_registration

EOF
    echo "Database $DATABASE loaded."

else
    echo "Skipping sqlite3 import."
fi

