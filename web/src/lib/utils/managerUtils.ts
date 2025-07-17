// Manager utility functions

export function getInitials(name: string): string {
	if (!name) return '';
	return name.split(' ').map(n => n.charAt(0)).join('').toUpperCase();
}

export function getManagerProfilePicture(managerName: string): string | null {
	// Check if managerName is valid
	if (!managerName || typeof managerName !== 'string') {
		console.warn('getManagerProfilePicture: Invalid managerName:', managerName);
		return null;
	}

	// Normalize manager name for file matching
	const normalizedName = managerName.toLowerCase().replace(/\s+/g, '-');
	
	// Define available profile pictures
	const availableProfiles: Record<string, string> = {
		'trevor': '/manager-profiles/trevor_cramer.jpeg',
		'luke-s': '/manager-profiles/luke_s.JPG',
		'israel': '/manager-profiles/israel_flores.jpg',
		'craig': '/manager-profiles/craig.jpeg',
		'nick': '/manager-profiles/nick.jpeg',
		'omar': '/manager-profiles/omar.jpg',
		'erik-snow': '/manager-profiles/erik.jpeg',
		'gabe-flores': '/manager-profiles/gabe_flores.jpeg',
		'gabe-the-younger': '/manager-profiles/gabe_the_younger.jpeg',
		'troy-colvin': '/manager-profiles/troy.jpeg',
		// Add more managers as their photos are added
	};

	console.log('getManagerProfilePicture Debug:', {
		managerName,
		normalizedName,
		availableProfiles,
		result: availableProfiles[normalizedName]
	});

	return availableProfiles[normalizedName] || null;
}

export function getTierColor(tier: string): string {
	switch (tier) {
		case 'League Alum': return 'bg-slate-400/20 text-slate-400 border-slate-400/40';
		case 'League Legend': return 'bg-yellow-400/20 text-yellow-400 border-yellow-400/40';
		case 'Dynasty Builder': return 'bg-purple-400/20 text-purple-400 border-purple-400/40';
		case 'Championship Elite': return 'bg-amber-400/20 text-amber-400 border-amber-400/40';
		case 'Champion': return 'bg-orange-400/20 text-orange-400 border-orange-400/40';
		case 'Elite Competitor': return 'bg-emerald-400/20 text-emerald-400 border-emerald-400/40';
		case 'Playoff Contender': return 'bg-blue-400/20 text-blue-400 border-blue-400/40';
		case 'League Regular': return 'bg-green-400/20 text-green-400 border-green-400/40';
		case 'Rebuilding': return 'bg-red-400/20 text-red-400 border-red-400/40';
		default: return 'bg-slate-400/20 text-slate-400 border-slate-400/40';
	}
}

export function getWinPercentageColor(winPct: number): string {
	if (winPct >= 0.70) return 'text-green-400';
	if (winPct >= 0.60) return 'text-blue-400';
	if (winPct >= 0.50) return 'text-amber-400';
	return 'text-red-400';
}

export function getChampionshipBadge(championships: number): string {
	if (championships >= 3) return '👑👑👑';
	if (championships === 2) return '👑👑';
	if (championships === 1) return '👑';
	return '';
} 