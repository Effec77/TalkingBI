import React from 'react';

interface GlassCardProps {
  children: React.ReactNode;
  className?: string;
}

const GlassCard: React.FC<GlassCardProps> = ({ children, className = "" }) => {
  return (
    <div 
      className={`bg-slate-900 border border-slate-800 rounded-xl shadow-lg p-6 ${className}`}
    >
      {children}
    </div>
  );
};

export default GlassCard;
