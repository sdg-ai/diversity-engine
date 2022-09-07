# diversity-engine
API and backend for the diversity project

### Set up for the project 

You will need to set a few environment variable:
* `SERVICE_ENDPOINT` (url to wasabi storage)
* `ACCESS_KEY_ID` (access key to wasabi storage)
* `SECRET_ACCESS_KEY` (secret key for wasabi storage)
* `PYTHONPATH=.:src`

For this, put all these variable in a `.env` file, and run:
`set -o allexport; source .env; set +o allexport`
