# Countries

### Description

Retrieve all countries with their properties.

## 1.1 CountryModel

```
{
    "avg_temperature": <float>,
    "capital": <string>,
    "code": <string>,
    "continent": <string>,
    "life_expectancy": <float>,
    "name": <string>,
    "population": <number>,
    "population_density": <number>
}
```

## 1.2 Get all countries

**Endpoint:** `/countries`

**Method:** `GET`

**Filter Url Parameter:**

Parameter Name | Parameter Type | Example
-------------- | -------------- | -------
country | string | `?country=austria`

**Response:** `200`

```
[
    <CountryModel>,
]
```

## 1.3 Get a country by name

**Endpoint:** `/countries?country=austria`

**Method:** `GET`

**Response:** `200`

```
<CountryModel>
```
