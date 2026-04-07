import React from 'react';

const StatusBanner: React.FC<{ status: string }> = ({ status }) => {
  if (!status) return null;

  const colors: Record<string, string> = {
    RESOLVED: "bg-[#e8f0ff] text-[#1f3a6b] border-[#b9ccef]",
    INCOMPLETE: "bg-[#fff6df] text-[#8a6a1f] border-[#f0dc9b]",
    AMBIGUOUS: "bg-[#fff0f2] text-[#9f3658] border-[#f1c6d3]",
    INVALID: "bg-[#fff0f2] text-[#9f3658] border-[#f1c6d3]",
    MODE_BLOCKED: "bg-[#eef2f9] text-[#4b5563] border-[#d0d9e8]",
  };

  const labels: Record<string, string> = {
    RESOLVED: "Success",
    INCOMPLETE: "Clarification Needed",
    AMBIGUOUS: "Multiple Paths Detected",
    INVALID: "Execution Failed",
    MODE_BLOCKED: "Restricted Mode",
  };

  return (
    <div className={`px-3 py-1 rounded-full text-[9px] font-bold uppercase tracking-widest border ${colors[status] || colors.MODE_BLOCKED}`}>
      {labels[status] || status}
    </div>
  );
};

export default StatusBanner;
