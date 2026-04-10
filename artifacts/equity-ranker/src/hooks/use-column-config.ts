import { useState, useCallback } from "react";

export const ALL_COLUMN_IDS = [
  "rank", "name", "sector", "price", "marketCap", "adv",
  "momentum6", "momentum12",
  "vol12", "alpha", "cluster", "quality",
] as const;

export type ColumnId = typeof ALL_COLUMN_IDS[number];

export const COLUMN_LABELS: Record<ColumnId, string> = {
  rank: "Rank",
  name: "Name",
  sector: "Sector",
  price: "Price",
  marketCap: "Mkt Cap",
  adv: "ADV",
  momentum6: "M6 / S6",
  momentum12: "M12 / S12",
  vol12: "Vol (12m)",
  alpha: "Alpha",
  cluster: "Group",
  quality: "Prof",
};

export const DEFAULT_VISIBLE: ColumnId[] = [
  "rank", "name", "sector", "price", "marketCap",
  "momentum6", "momentum12", "vol12", "alpha", "cluster",
];

const STORAGE_KEY = "qt:colConfig-v2";

interface ColConfig {
  visible: ColumnId[];
  order: ColumnId[];
}

function loadFromStorage(): ColConfig {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) {
      const parsed = JSON.parse(raw) as ColConfig;
      const knownSet = new Set<string>(ALL_COLUMN_IDS);
      const validOrder = (parsed.order || []).filter(id => knownSet.has(id)) as ColumnId[];
      const validVisible = (parsed.visible || []).filter(id => knownSet.has(id)) as ColumnId[];
      if (validOrder.length > 0 && validVisible.length > 0) {
        const missing = ALL_COLUMN_IDS.filter(id => !validOrder.includes(id));
        return {
          order: [...validOrder, ...missing],
          visible: [...validVisible, ...missing.filter(id => DEFAULT_VISIBLE.includes(id as ColumnId))],
        };
      }
    }
  } catch {}
  return { visible: [...DEFAULT_VISIBLE], order: [...ALL_COLUMN_IDS] };
}

function persist(cfg: ColConfig) {
  try { localStorage.setItem(STORAGE_KEY, JSON.stringify(cfg)); } catch {}
}

export function useColumnConfig() {
  const [config, setConfig] = useState<ColConfig>(() => loadFromStorage());

  const visibleSet = new Set(config.visible);
  const orderedVisible: ColumnId[] = config.order.filter(id => visibleSet.has(id));

  const toggleColumn = useCallback((id: ColumnId) => {
    setConfig(prev => {
      const next: ColConfig = {
        ...prev,
        visible: prev.visible.includes(id)
          ? prev.visible.filter(v => v !== id)
          : [...prev.visible, id],
      };
      persist(next);
      return next;
    });
  }, []);

  const moveColumn = useCallback((id: ColumnId, dir: "up" | "down") => {
    setConfig(prev => {
      const visSet = new Set(prev.visible);
      const visInOrder = prev.order.filter(v => visSet.has(v));
      const visIdx = visInOrder.indexOf(id);
      if (visIdx === -1) return prev;
      const toVisIdx = dir === "up" ? visIdx - 1 : visIdx + 1;
      if (toVisIdx < 0 || toVisIdx >= visInOrder.length) return prev;
      const neighborId = visInOrder[toVisIdx];
      const order = [...prev.order];
      const idxA = order.indexOf(id);
      const idxB = order.indexOf(neighborId);
      [order[idxA], order[idxB]] = [order[idxB], order[idxA]];
      const next = { ...prev, order };
      persist(next);
      return next;
    });
  }, []);

  const resetColumns = useCallback(() => {
    const next: ColConfig = { visible: [...DEFAULT_VISIBLE], order: [...ALL_COLUMN_IDS] };
    persist(next);
    setConfig(next);
  }, []);

  return { config, orderedVisible, toggleColumn, moveColumn, resetColumns };
}
