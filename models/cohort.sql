with
meds_at_discharge as (
  select
    hadm_id,
    sum(opioid_ind) as n_opioid_at_discharge,
    sum(benzo_ind) as n_benzo_at_discharge
  from
    {{ ref('medications')}} as medications
  where
    discharged_with_meds_ind = 1
  group by
    hadm_id
)
select
    distinct
    admission.*,
    meds_at_discharge.n_opioid_at_discharge,
    meds_at_discharge.n_benzo_at_discharge
from
    {{ ref('medications')}} as medications
    inner join {{ ref('admission') }} as admission
        on medications.hadm_id = admission.hadm_id
    left join meds_at_discharge
        on medications.hadm_id = meds_at_discharge.hadm_id
where
  (medications.pre_admit_meds_use_ind = 1 
  or medications.meds_start_during_admission_ind = 1)
  and medications.discharged_with_meds_ind = 1