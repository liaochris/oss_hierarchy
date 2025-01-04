### Overview
I cleaned names scraped from Linkedin profiles and used WRDS + Revelio Labs to obtain the linkedin profiles corresponding to those names. The obtained Linkedin profiles have not been processed yet and just represent a set of possible matches. 

### Source
Revelio Labs + WRDS

### When/where obtained & original form of files
The Linkedin profiles are obtained from the vendor Revelio Labs and accessed through the WRDS python API under the username `chrisliao`, which has an HBS affiliated WRDS account.

### Description
- `docs/get_linkedin_profile.py` queries WRDS for Linkedin Data
- `docs/wrds_tou.pdf` contains the WRDS TOU regarding data querying and usage. 

### Terms of Use
> The WRDS services are for academic and non-commercial research purposes only. Users may not use data downloaded from the WRDS database for any non-academic or commercial endeavor.
