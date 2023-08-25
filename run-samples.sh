BUILDIR=build/samples
AADIR=${BUILDIR}/annexa
CINDIR=${BUILDIR}/cin_census
SSDA903DIR=${BUILDIR}/ssda903

echo "Running Annex A samples"
mkdir -p ${AADIR}
python -m liiatools annex-a cleanfile \
    --i liiatools/spec/annex_a/samples/Annex_A.xlsx \
    --o ${AADIR} \
    --la_log_dir ${AADIR} \
    --la_code BAD 

python -m liiatools annex-a la-agg \
    --i ${AADIR}/Annex_A_clean.xlsx \
    --o ${AADIR} \

python -m liiatools annex-a pan-agg \
    --i ${AADIR}/AnnexA_merged.xlsx \
    --o ${AADIR} \
    --la_code BAD 

echo "Running CIN samples"
mkdir -p ${CINDIR}

python -m liiatools cin-census cleanfile \
    --i liiatools/spec/cin_census/samples/cin-2022.xml \
    --o ${CINDIR} \
    --la_log_dir ${CINDIR} \
    --la_code BAD

python -m liiatools cin-census la-agg \
    --i ${CINDIR}/cin-2022_clean.csv \
    --flat_output ${CINDIR} \
    --analysis_output ${CINDIR}

python -m liiatools cin-census pan-agg \
    --i ${CINDIR}/CIN_Census_merged_flatfile.csv \
    --flat_output ${CINDIR} \
    --analysis_output ${CINDIR} \
    --la_code BAD

echo "Running SSDA903 samples"
echo "*** NOTE: Only tests episodes file"
mkdir -p ${SSDA903DIR}

python -m liiatools s903 cleanfile \
    --i liiatools/spec/s903/samples/SSDA903_2020_episodes.csv \
    --o ${SSDA903DIR} \
    --la_log_dir ${SSDA903DIR} \
    --la_code BAD

python -m liiatools s903 la-agg \
    --i ${SSDA903DIR}/SSDA903_2020_episodes_clean.csv \
    --o ${SSDA903DIR} 

python -m liiatools s903 pan-agg \
    --i ${SSDA903DIR}/SSDA903_Episodes_merged.csv \
    --o ${SSDA903DIR} \
    --la_code BAD