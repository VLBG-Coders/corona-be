# Cases Total

### Description

Retrieve a total of all cases for each country.

## 1.1 CasesTotalModel

```
{
    "confirmed": <number>,
    "date": <string>,
    "deaths": <number>,
    "delta_confirmed": <number>,
    "delta_recovered": <number>,
    "recovered": <number>
}
```

## 1.2 Get total cases worldwide

**Endpoint:** `/cases-total`

**Method:** `GET`

**Filter Url Parameter:**

Parameter Name | Parameter Type | Example
-------------- | -------------- | -------
country | string | `?country=austria`

**Response:** `200`

```
[
    {
        "country": <CountryModel>,
        "cases": [
            <CasesTotalModel>
        ]    
    }
]
```

## 1.3 Get total cases for a country

**Endpoint:** `/cases-total?country=austria`

**Method:** `GET`

**Filter Url Parameter:**

Parameter Name | Parameter Type | Example
-------------- | -------------- | -------
country | string | `?country=austria`

**Response:** `200`

```
{
    "country": <CountryModel>,
    "cases": [
        <CasesTotalModel>
    ]
}
```
