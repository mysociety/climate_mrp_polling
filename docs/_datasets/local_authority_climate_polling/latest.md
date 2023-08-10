---
name: local_authority_climate_polling
title: Local authority climate polling
description: "Reformatted MRP pollings for local authorities areas\n"
version: latest
contributors:
- title: mySociety
  path: https://mysociety.org
  role: author
licenses:
- name: notspecified
  title: Not specified
custom:
  build: climate_mrp_polling.convert_specific_polling:convert_all
  tests:
  - test_local_authority_climate_polling
  dataset_order: 0
  download_options:
    gate: default
    survey: default
    header_text: default
  formats:
    csv: true
    parquet: true
  composite:
    xlsx:
      include: all
      exclude: none
      render: true
    sqlite:
      include: all
      exclude: none
      render: true
    json:
      include: all
      exclude: none
      render: true
  change_log:
    0.1.0: ''
    0.1.1: 'Minor change in data for resource(s): local_authority_climate_polling,local_authority_climate_polling_guide'
resources:
- title: Local authority climate polling
  description: "Reformatted MRP pollings for local authorities areas\n"
  custom:
    row_count: 42020
  path: local_authority_climate_polling.csv
  name: local_authority_climate_polling
  profile: tabular-data-resource
  scheme: file
  format: csv
  hashing: md5
  encoding: utf-8
  schema:
    fields:
    - name: source
      type: string
      description: Source of the polling
      constraints:
        unique: false
        enum:
        - Onward2022
        - RenewableUK2022
      example: Onward2022
    - name: local-authority-code
      type: string
      description: Local authority code (3/4 letter)
      constraints:
        unique: false
      example: ABD
    - name: official-name
      type: string
      description: Official name of the local authority
      constraints:
        unique: false
      example: Aberdeen City Council
    - name: question
      type: string
      description: Question short key
      constraints:
        unique: false
      example: Q01_CON
    - name: percentage
      type: number
      description: Percentage of people who agreed with question statement
      constraints:
        unique: false
      example: 1.2513365984620128e-05
  hash: f9123be07960702c0998285469b636be
- title: Local authority climate polling guide
  description: Map of questions to short keys used in polling
  custom:
    row_count: 111
  path: local_authority_climate_polling_guide.csv
  name: local_authority_climate_polling_guide
  profile: tabular-data-resource
  scheme: file
  format: csv
  hashing: md5
  encoding: utf-8
  schema:
    fields:
    - name: source
      type: string
      description: Source of the polling
      constraints:
        unique: false
        enum:
        - Onward2022
        - RenewableUK2022
      example: Onward2022
    - name: question
      type: string
      description: Full question text
      constraints:
        unique: true
      example: "\"How seriously do you take the issue of climate change?\" -- Don't\
        \ Know"
    - name: short
      type: string
      description: Short key for the question
      constraints:
        unique: true
      example: Q01_CON
  hash: 37ec9941effde266e53ab7bd74a7c567
full_version: 0.1.1
permalink: /datasets/local_authority_climate_polling/latest
---
