$ADMIN import \
--database pj100k \
--verbose \
--nodes=node_conditions.csv \
--nodes=node_encounters.csv \
--nodes=node_patients.csv \
--relationships=rel_patient_encounter.csv \
--relationships=rel_encounter_conditions.csv \
--relationships=rel_encounter_next.csv