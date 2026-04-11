import { useState, useCallback } from "react";

const STORAGE_KEY = "qt:hidden-tags-v1";

function loadFromStorage(): Set<string> {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) return new Set<string>(parsed);
    }
  } catch {}
  return new Set<string>();
}

function persist(hidden: Set<string>) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify([...hidden]));
  } catch {}
}

export function useHiddenTags() {
  const [hiddenTags, setHiddenTags] = useState<Set<string>>(() => loadFromStorage());

  const toggleHide = useCallback((tagKey: string) => {
    setHiddenTags(prev => {
      const next = new Set(prev);
      if (next.has(tagKey)) {
        next.delete(tagKey);
      } else {
        next.add(tagKey);
      }
      persist(next);
      return next;
    });
  }, []);

  const isHidden = useCallback((tagKey: string) => hiddenTags.has(tagKey), [hiddenTags]);

  return { hiddenTags, toggleHide, isHidden };
}
