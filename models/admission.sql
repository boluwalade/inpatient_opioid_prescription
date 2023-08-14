with
admission_rank as (
  select
    subject_id,
    hadm_id,
    admittime,
    dischtime,
    language,
    ethnicity,
    diagnosis,
    rank() over (
      partition by subject_id
      order by admittime
      ) as ranking
  from
    `physionet-data.mimiciii_clinical.admissions`
  where
    hospital_expire_flag = 0 --patients who didnt expire during admission
    and extract( year from admittime) between 2112 and 2132
)
select 
  admission_rank.subject_id,
  admission_rank.hadm_id,
  admission_rank.admittime,
  admission_rank.dischtime,
  admission_rank.language,
  admission_rank.ethnicity,
  string_agg(admission_rank.diagnosis) as visit_diagnosis,
  datetime_diff(admission_rank.admittime,patients.dob, year) as age_at_admission
from
  admission_rank
  inner join `physionet-data.mimiciii_clinical.patients` as patients
    on admission_rank.subject_id = patients.subject_id
where
  datetime_diff(admission_rank.dischtime,admission_rank.admittime, day) <= 120 --LOS <= 120
  and extract( year from admission_rank.admittime) = extract( year from admission_rank.dischtime) --same admit and discharge year
group by
  admission_rank.subject_id,
  admission_rank.hadm_id,
  admission_rank.admittime,
  admission_rank.dischtime,
  patients.dob,
  admission_rank.language,
  admission_rank.ethnicity
having
  age_at_admission > 18
