 select
    prescriptions.row_id,
    prescriptions.subject_id,
    prescriptions.hadm_id,
    prescriptions.startdate,
    prescriptions.enddate,
    case
      when
        lower(drug) like '%fentanyl%'
        or lower(drug) like '%oxycodone%'
        or lower(drug) like '%hydromorphone%'
        or lower(drug) like '%tramadol%'
        or lower(drug) like '%buprenorphine%'
        or lower(drug) like '%methadone%'
        or lower(drug) like '%tapentadol%'
        or lower(drug) like '%morphine%'
        or lower(drug) like '%oxymorphone%'
        or lower(drug) like '%hydrocodone%'
        or lower(drug) like '%meperidine%'
        or lower(drug) like '%butorphanol%'
        or lower(drug) like '%codeine%'
        or lower(drug) like '%nalbuphine%'
        or lower(drug) like '%opium%'
        or lower(drug) like '%propoxyphene%'
        or lower(drug) like '%levorphanol%'
        or lower(drug) like '%pentazocine%'
        or lower(drug) like '%alfentanil%'
        or lower(drug) like '%sufentanil%'
        or lower(drug) like '%remifentanil%'
        or lower(drug) like '%oliceridine%'
          then 1
        else 0
      end
    as opioid_ind,
    case
        when
          lower(drug) like '%alprazolam%'
          or lower(drug) like '%clonazepam%'
          or lower(drug) like '%lorazepam%'
          or lower(drug) like '%diazepam%'
          or lower(drug) like '%temazepam%'
          or lower(drug) like '%chlordiazepoxide%'
          or lower(drug) like '%midazolam%'
          or lower(drug) like '%flurazepam%'
          or lower(drug) like '%oxazepam%'
          or lower(drug) like '%triazolam%'
          or lower(drug) like '%clorazepate%'
          or lower(drug) like '%clobazam%'
          or lower(drug) like '%estazolam%'
          or lower(drug) like '%quazepam%'
          or lower(drug) like '%remimazolam%'
            then 1
        else 0
      end
    as benzo_ind,
    case
      when admission.admittime >= prescriptions.startdate
        then 1
      else 0
    end as pre_admit_meds_use_ind,
    case
      when admission.admittime <= prescriptions.startdate
        and admission.dischtime >= prescriptions.startdate
        then 1
      else 0
    end as meds_start_during_admission_ind,
    case
      when admission.dischtime <= prescriptions.enddate
        or prescriptions.enddate is null
          then 1
      else 0
    end as discharged_with_meds_ind
  from
    {{ ref('admission') }} as admission
    inner join `physionet-data.mimiciii_clinical.prescriptions` as prescriptions
      on admission.subject_id = prescriptions.subject_id --prescriptions during admission
  where (
    lower(drug) like '%fentanyl%'
    or lower(drug) like '%oxycodone%'
    or lower(drug) like '%hydromorphone%'
    or lower(drug) like '%tramadol%'
    or lower(drug) like '%buprenorphine%'
    or lower(drug) like '%methadone%'
    or lower(drug) like '%tapentadol%'
    or lower(drug) like '%morphine%'
    or lower(drug) like '%oxymorphone%'
    or lower(drug) like '%hydrocodone%'
    or lower(drug) like '%meperidine%'
    or lower(drug) like '%butorphanol%'
    or lower(drug) like '%codeine%'
    or lower(drug) like '%nalbuphine%'
    or lower(drug) like '%opium%'
    or lower(drug) like '%propoxyphene%'
    or lower(drug) like '%levorphanol%'
    or lower(drug) like '%pentazocine%'
    or lower(drug) like '%alfentanil%'
    or lower(drug) like '%sufentanil%'
    or lower(drug) like '%remifentanil%'
    or lower(drug) like '%oliceridine%'
    or lower(drug) like '%alprazolam%'
    or lower(drug) like '%clonazepam%'
    or lower(drug) like '%lorazepam%'
    or lower(drug) like '%diazepam%'
    or lower(drug) like '%temazepam%'
    or lower(drug) like '%chlordiazepoxide%'
    or lower(drug) like '%midazolam%'
    or lower(drug) like '%flurazepam%'
    or lower(drug) like '%oxazepam%'
    or lower(drug) like '%triazolam%'
    or lower(drug) like '%clorazepate%'
    or lower(drug) like '%clobazam%'
    or lower(drug) like '%estazolam%'
    or lower(drug) like '%quazepam%'
    or lower(drug) like '%remimazolam%'
    )