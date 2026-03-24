import os

import requests

BASE_URL = "https://listmonk-demo-afhcg8e2hde0fxca.eastus2-01.azurewebsites.net/api"  # noqa

GIULIA_ONLY_LIST_ID = 21
GIULIA_SUBSCRIBER_ID = "giulia.martini@un.org"
#DSCI_LIST_ID = 6

BASE_CAMPAIGN_ID = 100
BASE_TRANSACTIONAL_ID = 100

USERNAME_LISTMONK = os.getenv("DSCI_LISTMONK_API_USERNAME")
PASSWORD_LISTMONK = os.getenv("DSCI_LISTMONK_API_KEY")


def create_campaign(
    name: str = "test_campaign",
    subject: str = "Test Subject",
    list_ids: list[int] = None,
    template_id: int = BASE_CAMPAIGN_ID,
    body: str = "TEST CONTENT",
):
    if list_ids is None:
        list_ids = [GIULIA_ONLY_LIST_ID]
    create_payload = {
        "name": name,
        "subject": subject,
        "lists": list_ids,  # list IDs to send to
        "template_id": template_id,  # your template ID
        "type": "regular",
        "content_type": "html",
        "body": body,
    }

    r = requests.post(
        f"{BASE_URL}/campaigns", auth=(USERNAME_LISTMONK, PASSWORD_LISTMONK), json=create_payload
    )

    r.raise_for_status()
    campaign = r.json()["data"]
    campaign_id = campaign["id"]
    return campaign_id


def send_campaign(campaign_id: int):
    r = requests.put(
        f"{BASE_URL}/campaigns/{campaign_id}/status",
        auth=(USERNAME_LISTMONK, PASSWORD_LISTMONK),
        json={"status": "running"},
    )
    r.raise_for_status()


def send_transactional(
    to_emails: list[tuple[str, str]],
    subject: str,
    template_id: int = BASE_TRANSACTIONAL_ID,
    cc_emails: list[tuple[str, str]] = None,
    data: dict = None,
):
    payload = {
        "subscriber_email": to_emails[0][1],  # primary recipient
        "template_id": template_id,
        "from_email": "OCHA Data Science <ocha-datascience@un.org>",
        "content_type": "html",
        "subject": subject,
        "data": data or {},
        "headers": [
            {
                "To": ", ".join(
                    [f"{name} <{email}>" for name, email in to_emails]
                )
            },
            {
                "Cc": ", ".join(
                    [f"{name} <{email}>" for name, email in cc_emails or []]
                )
            },
        ],
    }

    r = requests.post(
        f"{BASE_URL}/tx", auth=(USERNAME_LISTMONK, PASSWORD_LISTMONK), json=payload
    )
    if not r.ok:
        print("Status:", r.status_code)
        print("Response text:", r.text)
        print(
            "Response JSON:",
            r.json()
            if "application/json" in r.headers.get("Content-Type", "")
            else None,
        )
        print("Payload sent:", payload)

    r.raise_for_status()
    return r.json()

def generate_body_email(storm_name, date_myanmar):
    HTML_INTRO = f"""
    {storm_name} - {date_myanmar} (Myanmar local time) <br>
    Dear colleagues,<br>
    SOME CONTENT
    <br><br>
    """

    HTML_CONCLUSION = """
    <br>
    This email is purely informational and does not serve as an official notice for the anticipatory action framework. Official activation notices are sent in another email.
    <br>
    The code used to produce this alert is available on GitHub <a  href="https://github.com/OCHA-DAP/ds-aa-mmr-cyclones/">here</a>.
    <br><br>
    Best regards,<br>
    OCHA Centre for Humanitarian Data
    """
    return HTML_INTRO + HTML_CONCLUSION