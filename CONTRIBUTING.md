# 👋 Welcome

Hello and welcome! Thank you so much for taking the time to contribute to the Tuva Project. People like you are helping to build a community of healthcare data practitioners that shares knowledge and tools. Whether it’s fixing a bug, submitting an idea, updating the docs, or sharing your healthcare knowledge, you can make an impact!

In this guide, you will get an overview of the contribution workflow, from how to contribute, setting up your development environment, testing, and creating a pull request.

# 🤝 How to contribute

There are many different ways to contribute to the Tuva Project. The goal of this section is to help you get started.

The Tuva Project is organized across a few different [repositories](https://github.com/orgs/tuva-health/repositories) on GitHub:

- **[The Tuva Project](https://github.com/tuva-health/the_tuva_project):** main repository with a dbt package that contains data marts, terminology codes set, and data quality tests for transforming healthcare data.
- **[The Tuva Project Demo](https://github.com/tuva-health/the_tuva_project_demo):** starter dbt project with synthetic claims data for trying out the Tuva Project.
- **[Connectors](https://github.com/orgs/tuva-health/repositories?q=connector&type=all&language=&sort=):** connectors that map healthcare data to the Tuva claims data model so you can easily run the Tuva Project. (e.g. [medicare_cclf_connector](https://github.com/tuva-health/medicare_cclf_connector), [medicare_lds_connector](https://github.com/tuva-health/medicare_lds_connector), [fhir_connector](https://github.com/tuva-health/FHIR_connector))
- **[Docs](https://github.com/tuva-health/docs):** contains all of the [thetuvaproject.com](https://thetuvaproject.com/) code, including documentation on how to map your data, run the Tuva Project, and deep-dives into advanced healthcare concepts.
- **[Provider](https://github.com/tuva-health/provider):** dbt project that transforms messy public provider datasets into usable data for the Tuva Project. This is how the provider terminology seed files that come with the Tuva Project are created.

### Work on an existing issue

You can choose an existing issue to work on from the main [repository](https://github.com/tuva-health/the_tuva_project/issues). This is where we track bugs and feature requests. Make sure to comment on the issue so we know you’re working on it and can help if you have questions.

### File a bug report or request a new feature

Let us know if something is not working or request a new feature. Go to the [issues](https://github.com/tuva-health/the_tuva_project/issues) of the main repository and create a new issue.

You can also take a look at the roadmap on our [Docs](https://thetuvaproject.com/) to see what else we have planned. If a planned feature is interesting to you, you can create a new issue where we can collaborate on it!

### Update documentation

One of the tenets of the Tuva Project is sharing healthcare data knowledge, and part of that is posting this knowledge on the **[Docs](https://github.com/tuva-health/docs)** website. There are many ways to contribute here; you do not have to know markdown language. We can help with any tricky formatting. You can go to an existing page on the docs website and click “Edit this page.” This will take you to GitHub, where you can fork the repo and create a PR with your changes.

### Share your knowledge

Another great way to contribute is to join our growing community of healthcare data practitioners in [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)! Here, we are trying to foster an open environment where we can share ideas and collaborate.

### Use the Tuva Project

Lastly, using the Tuva Project and sending your feedback is one of the most valuable ways to contribute. 

### Not sure where to start?

Join our [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q) community, and we will help you get started! You can also help by giving us a  ⭐ on [GitHub](https://github.com/tuva-health/the_tuva_project) and telling your friends and colleagues about the Tuva Project.

# 🛠️ Getting started with development

### How to setup your environment

1. In order to run the Tuva Project, you need to have dbt installed and healthcare data loaded inside a data warehouse that we support.
    1. If you’re new to dbt, check out their [Quickstart guides](https://docs.getdbt.com/quickstarts). We currently support version 1.3.X or greater. You can use either [dbt cloud](https://cloud.getdbt.com/) or [dbt CLI](https://docs.getdbt.com/dbt-cli/cli-overview).
    2. We currently support BiqQuery, Redshift, and Snowflake.
    3. If you do not have access to healthcare data, feel free to use [The Tuva Project Demo](https://github.com/tuva-health/the_tuva_project_demo).
2. [Fork](https://github.com/tuva-health/the_tuva_project/fork) the repository you would like to contribute to and begin developing.

### How to test the package

The easiest way to test your changes is to use the dbt project inside the package called [integration_tests](https://github.com/tuva-health/the_tuva_project/tree/main/integration_tests).

1. Set the project subdirectory to “integration_tests” if using dbt cloud or change directory (`cd integration_tests`) if using CLI.
2. Choose a data source:
   1. To use synthetic demo data:
        -  Set test_data_override to true
   3. To use your own data sources, update the vars in integration_tests/dbt_project.yml:
        - Set input_database and input_schema to your testing sources
4. Run `dbt deps`.
5. Run `dbt build`.

You only need to test your changes in one data warehouse. When you submit your pull request, we will use our automated CI testing workflows to test all of our supported data warehouses.

### Submitting your changes

When you are ready, create a pull request in GitHub using our template. See GitHub’s [guide](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork) for help with creating pull requests from a fork. We will work with you if anything comes up during our review and testing. Once your PR is merged, your contributions will be publicly visible on the Tuva Project repositories.

👏 That’s it; you just contributed to your first open-source project! If you need any help, please reach out to us on [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q).
