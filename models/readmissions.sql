select
    cohort.subject_id,
    cohort.hadm_id,
    max(
        case
            when cohort.dischtime < admissions.admittime
                then 1
            else 0
        end
    ) as readmission_ind,
    max(
        case
            when cohort.dischtime < admissions.admittime
            and date_add(cohort.dischtime, interval 30 day) >= admissions.admittime
                then 1
            else 0
        end
    ) as readmission_30_days,
    max(
        case
            when cohort.dischtime < admissions.admittime
            and date_add(cohort.dischtime, interval 60 day) >= admissions.admittime
                then 1
            else 0
        end
    ) as readmission_60_days,
    max(
        case
            when cohort.dischtime < admissions.admittime
            and date_add(cohort.dischtime, interval 90 day) >= admissions.admittime
                then 1
            else 0
        end
    ) as readmission_90_days,
    string_agg(diagnosis) as readmission_reason
from
    {{ ref('cohort') }} as cohort
    inner join `physionet-data.mimiciii_clinical.admissions` as admissions
        on cohort.subject_id = admissions.subject_id
group by
    subject_id,
    hadm_id