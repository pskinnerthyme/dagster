import {AssetGraphFilterBar} from 'shared/asset-graph/AssetGraphFilterBar.oss';
import {useAssetCatalogFiltering} from 'shared/assets/useAssetCatalogFiltering.oss';

import {AssetGraphViewType, GraphNode} from './Utils';

export type Props = {
  nodes: GraphNode[];
  clearAssetSelection: () => void;
  setAssetSelection: (selection: string) => void;
  assetSelection: string;
  viewType: AssetGraphViewType;
  loading: boolean;
};

export function useAssetGraphExplorerFilters({
  nodes,
  viewType,
  assetSelection,
  loading,
  clearAssetSelection,
}: Props) {
  const ret = useAssetCatalogFiltering({
    assets: nodes,
    includeRepos: viewType === AssetGraphViewType.GLOBAL,
    loading,
  });

  return {
    ...ret,
    button: ret.filterButton,
    filterBar: (
      <AssetGraphFilterBar
        activeFiltersJsx={ret.activeFiltersJsx}
        assetSelection={assetSelection}
        clearAssetSelection={clearAssetSelection}
      />
    ),
  };
}
