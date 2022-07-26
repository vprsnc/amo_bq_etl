import pandas as pd


def parse_leads(leads):

    df = pd.DataFrame(leads)

    df ["data_obrasheniya"] = df["Дата прихода обращения"].fillna(0).astype("int")

    df.drop([
        "_links", "_embedded", "custom_fields_values", "Дата прихода обращения"
       ], axis=1, inplace=True)

    df["created_at"] = pd.to_datetime(df["created_at"], unit="s")
    df["updated_at"] = pd.to_datetime(df["updated_at"], unit="s")
    df["closed_at"]  = pd.to_datetime(df["closed_at"], unit="s")

    df.rename({
        "utm_campaign (2)": "utm_campaign_2",
        "utm_term (2)": "utm_term_2"
       }, axis=1, inplace=True)

    return df

def parse_status_changes(events):

    df = pd.DataFrame(events)

    df.drop([
        "value_after", "value_before", "_links", "_embedded"
       ], axis=1, inplace=True)
    df["created_at"] = pd.to_datetime(df["created_at"], unit="s")

    return df
