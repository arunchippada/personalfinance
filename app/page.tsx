import { PortfolioBuilder } from "@/components/portfolio-builder";
import { loadPortfolioData } from "@/lib/fund-data";

export default async function Page() {
  const data = await loadPortfolioData();

  return (
    <PortfolioBuilder
      csvPath={data.csvPath}
      funds={data.funds}
      rowCount={data.rowCount}
      warnings={data.warnings}
    />
  );
}
