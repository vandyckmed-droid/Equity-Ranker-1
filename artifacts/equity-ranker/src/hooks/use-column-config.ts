import { useState, useCallback } from "react";

export const ALL_COLUMN_IDS = [
  "rank", "name", "sector", "price", "marketCap", "adv",
  "momentum6", "momentum12", "quality", "vol12", "alpha", "cluster",
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
  quality: "Quality",
  vol12: "Vol (12m)",
  alpha: "Alpha",
  cluster: "Cluster",
};

export const DEFAULT_VISIBLE: ColumnId[] = [
  "rank", "name", "sector", "price", "marketCap",
  "momentum6", "momentum12", "quality", "vol12", "alpha", "cluster",
];

const STORAGE_KEY = "qt:colConfig";

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
      const order = [...prev.order];
      const idx = order.indexOf(id);
      if (idx === -1) return prev;
      const to = dir === "up" ? idx - 1 : idx + 1;
      if (to < 0 || to >= order.length) return prev;
      [order[idx], order[to]] = [order[to], order[idx]];
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
