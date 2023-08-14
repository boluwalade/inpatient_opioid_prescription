select
    patients.*
from
    `physionet-data.mimiciii_clinical.patients` as patients
    inner join {{ ref('cohort')}} as cohort
        on patients.subject_id = cohort.subject_id