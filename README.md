<img
    src="./assets/images/DSB__Logo_Navnetrekk_Hvit.svg"
    style="
        background: #1A1A1A;
        padding: 1em;
        border-radius: 10px;
        max-width: 500px;
    "
/>

# Introduction
This is the repository for integration guides/documentation for [DSB](https://www.dsb.no/)
<br>(Direktoratet for samfunnssikkerhet og beredskap / Direktoratet for samfunnssikkerhet og beredskap / The Norwegian Directorate for Civil Protection)

Here you can find information and examples detailing the avenues we provide for integrations.

## Who is this for?

The code and examples here expects you to have some fundamental programming knowledge, but we try to keep it as simple as possible.
<br>Our goal is that it should be easy for programmers to follow, and possible for technically inclined users to use with a little effort.

# Guides
- [Dataplatform](./dataplatform/README.md)

<br><br><br><br>
## For developers on this repository
Register this filter to automatically clear output from Jupyter notebooks when committing them to the repository.
``` bash
git config filter.strip-notebook-output.clean 'jupyter nbconvert --ClearOutputPreprocessor.enabled=True --to=notebook --stdin --stdout --log-level=ERROR'  
```