"use client";

import { useEffect } from "react";

export function IframeResizerChild() {
  useEffect(() => {
    // Only initialize if we're in an iframe
    if (window.self !== window.top) {
      import("@iframe-resizer/child");
    }
  }, []);

  return null;
}
