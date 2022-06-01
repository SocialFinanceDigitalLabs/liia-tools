# Set of lists for ensuring that dates are read as dates, and that the aggregated
# CIN Census files don't contain copies of the same events #

dates = ['Date', 'CINreferralDate', 'CINclosureDate', 'DateOfInitialCPC', 'CINPlanStartDate', 'CINPlanEndDate', 'S47ActualStartDate', 'InitialCPCtarget', 'AssessmentActualStartDate', 'AssessmentInternalReviewDate', 'AssessmentAuthorisationDate', 'CPPstartDate', 'CPPendDate', 'PersonBirthDate', 'ExpectedPersonBirthDate', 'PersonDeathDate']

sort_order = ['InitialCPCtarget', 'AssessmentInternalReviewDate', 'YEAR']

dedup = ['Date', 'Type', 'LAchildID', 'ReferralSource']

# Assumptions used in producing analytical outputs of CIN Census

ref_assessment = 30

s47_cpp_days = 60

icpc_cpp_days = 45