"""set the allow_field_overrides flag in postgres profiles' target configs"""

import argparse
import asyncio
from prefect_dbt.cli import DbtCliProfile

parser = argparse.ArgumentParser()
parser.add_argument("--cli-block-name", required=True)
parser.add_argument("--show", action="store_true")
parser.add_argument("--allow-field-overrides", action="store_true")
args = parser.parse_args()


async def main():
    """main function"""
    dbt_cli_profile = await DbtCliProfile.load(args.cli_block_name)
    if args.show:
        print(f"profile={dbt_cli_profile.get_profile()}")
        print(f"target_configs.type={dbt_cli_profile.target_configs.type}")
        print(f"target_configs.schema={dbt_cli_profile.target_configs.schema}")
        print(
            f"target_configs.allow_field_overrides={dbt_cli_profile.target_configs.allow_field_overrides}"
        )
        print(f"target_configs.extras={dbt_cli_profile.target_configs.extras}")

    if args.allow_field_overrides:
        dbt_cli_profile.target_configs.allow_field_overrides = True
        await dbt_cli_profile.save(name=args.cli_block_name, overwrite=True)
        print("allow_field_overrides set to True")


if __name__ == "__main__":
    asyncio.run(main())
