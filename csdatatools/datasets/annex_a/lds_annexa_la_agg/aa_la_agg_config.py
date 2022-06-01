# Set of dictionaries for matching List against relevant variables for the process of
# aggregating a single LA's Annex A files together #

dates = {
    'List 1' : ['Date of Birth', 'Date of Contact'],
    'List 2' : ['Date of Birth', 'Assessment start date', 'Assessment completion date'],
    'List 3' : ['Date of Birth', 'Date of referral'],
    'List 4' : ['Date of Birth', 'Continuous Assessment Start Date', 'Continuous Assessment Date of Authorisation'],
    'List 5' : ['Date of Birth', 'Strategy discussion initiating Section 47 Enquiry Start Date', 'Date of Initial Child Protection Conference'],
    'List 6' : ['Date of Birth', 'CIN Start Date', 'Date Child Was Last Seen', 'CIN Closure Date'],
    'List 7' : ['Date of Birth', 'Child Protection Plan Start Date', 'Date of the Last Statutory Visit', 'Date of latest review conference', 'Child Protection Plan End Date'],
    'List 8' : ['Date of Birth', 'Date Started to be Looked After', 'Start date of current legal status', 'Date of Latest Statutory Review', 'Date of Last Social Work Visit', 'Date of Last IRO Visit / Contact to the Child', 'Date of Last Health Assessment', 'Date of Last Dental Check', 'Date Ceased to be Looked After', 'Start Date of Most Recent Placement'],
    'List 9' : ['Date of Birth'],
    'List 10' : ['Date of Birth', 'Date the Child Entered Care', 'Date of Decision that Child Should be Placed for Adoption', 'Date of Placement Order', 'Date of Matching Child and Prospective Adopters', 'Date Placed for Adoption', 'Date of Adoption Order', 'Date of Decision that Child Should No Longer be Placed for Adoption', 'Date the child was placed for fostering in FFA or concurrent planning placement'],
    'List 11' : ['Date enquiry received', 'Date Stage 1 started', 'Date Stage 1 ended', 'Date Stage 2 started', 'Date Stage 2 ended', 'Date application submitted', 'Date application approved', 'Date adopter matched with child(ren)', 'Date child/children placed with adopter(s)', 'Date of Adoption Order', 'Date of leaving adoption process']
    }

dedup = {
    'List 1' : ['Child Unique ID', 'Date of Contact', 'Contact Source'],
    'List 2' : ['Child Unique ID', 'Assessment start date'],
    'List 3' : ['Child Unique ID', 'Date of referral', 'Referral Source'],
    'List 4' : ['Child Unique ID', 'Continuous Assessment Start Date'],
    'List 5' : ['Child Unique ID', 'Strategy discussion initiating Section 47 Enquiry Start Date'],
    'List 6' : ['Child Unique ID', 'CIN Start Date'],
    'List 7' : ['Child Unique ID', 'Child Protection Plan Start Date'],
    'List 8' : ['Child Unique ID', 'Date Started to be Looked After', "Child's Legal Status", 'Placement Type', 'Placement Provider', 'Placement postcode'],
    'List 9' : 'Child Unique ID',
    'List 10' : ['Child Unique ID', 'Date of Decision that Child Should be Placed for Adoption'],
    'List 11' : ['Individual adopter identifier', 'Date enquiry received']
}

index_date = {
    'List 1' : 'Date of Contact',
    'List 2' : 'Assessment completion date',
    'List 3' : 'Date of referral',
    'List 4' : 'Continuous Assessment Date of Authorisation',
    'List 5' : 'Strategy discussion initiating Section 47 Enquiry Start Date',
    'List 6' : 'CIN Closure Date',
    'List 7' : 'Child Protection Plan End Date',
    'List 8' : 'Date Ceased to be Looked After',
    'List 9' : 'Date of Birth',
    'List 10' : ['Date of Decision that Child Should be Placed for Adoption', 'Date of Adoption Order', 'Date of Decision that Child Should No Longer be Placed for Adoption'],
    'List 11' : 'Date enquiry received'
}