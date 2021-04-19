## Transform-ingress2event-time Chart

### Usage
This chart comes unconfigured and will need to be configured with the following values to work.

Undefined values:
```image.repository```
```image.tag```

There are some undefined values ```inside the config.conf.ini```
The undefined values are:
* ```filesystem_name ```
* ```source```
* ```destination```
* ```date_key_name```


### Values

| Parameter | Description | Default |
|-----------|-------------|---------|
| `appName` | The overall name | osiris-egress
| `image.repository` | The repository of the image | nil
| `image.tag` | The tag of the image | latest
| `schedule` | Cron schedule | "*/15 * * * *"
| `transformationparams.datestring` | The ingestion time / start time | empty string
| `config.'conf.ini'` | Config for the app | see [here](https://github.com/Open-Dataplatform/osiris-ingress-api/#configuration)
| `config.'log.conf'` | Logging config for the app | see [here](https://github.com/Open-Dataplatform/osiris-ingress-api/#configuration)
