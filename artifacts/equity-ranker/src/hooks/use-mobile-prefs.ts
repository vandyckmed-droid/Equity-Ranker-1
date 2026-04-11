import { useState, useCallback } from "react";

const STORAGE_KEY = "qt:mobile-prefs-v1";

interface MobilePrefs {
  showGroup: boolean;
  showSuggestedWeight: boolean;
}

const DEFAULTS: MobilePrefs = {
  showGroup: true,
  showSuggestedWeight: true,
};

function load(): MobilePrefs {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return { ...DEFAULTS };
    const parsed = JSON.parse(raw) as Partial<MobilePrefs>;
    return {
      showGroup: parsed.showGroup ?? DEFAULTS.showGroup,
      showSuggestedWeight: parsed.showSuggestedWeight ?? DEFAULTS.showSuggestedWeight,
    };
  } catch {
    return { ...DEFAULTS };
  }
}

function save(prefs: MobilePrefs): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(prefs));
  } catch {
  }
}

export function useMobilePrefs() {
  const [prefs, setPrefs] = useState<MobilePrefs>(() => load());

  const toggle = useCallback((key: keyof MobilePrefs) => {
    setPrefs(prev => {
      const next = { ...prev, [key]: !prev[key] };
      save(next);
      return next;
    });
  }, []);

  return {
    showGroup: prefs.showGroup,
    showSuggestedWeight: prefs.showSuggestedWeight,
    toggleShowGroup: () => toggle("showGroup"),
    toggleShowSuggestedWeight: () => toggle("showSuggestedWeight"),
  };
}
