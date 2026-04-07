import React from 'react';

const StatusBanner: React.FC<{ status: string }> = ({ status }) => {
  if (!status) return null;

  const colors: Record<string, string> = {
    RESOLVED: "bg-[#a0caff]/10 text-[#a0caff] border-[#a0caff]/30",
    INCOMPLETE: "bg-[#fbbb45]/10 text-[#fbbb45] border-[#fbbb45]/30",
    AMBIGUOUS: "bg-[#ffb4ab]/10 text-[#ffb4ab] border-[#ffb4ab]/30",
    INVALID: "bg-[#ffb4ab]/10 text-[#ffb4ab] border-[#ffb4ab]/30",
    MODE_BLOCKED: "bg-[#414751]/20 text-[#c1c7d2] border-[#414751]/30",
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
