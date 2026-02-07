'use client'

import { useEffect, useState, useMemo } from 'react'
import dynamic from 'next/dynamic'
import { ConstituencyWinner, getAllianceColor, ALLIANCE_COLORS } from '@/lib/supabase'

// Dynamically import react-leaflet components (required for SSR)
const MapContainer = dynamic(
  () => import('react-leaflet').then((mod) => mod.MapContainer),
  { ssr: false }
)
const TileLayer = dynamic(
  () => import('react-leaflet').then((mod) => mod.TileLayer),
  { ssr: false }
)
const GeoJSON = dynamic(
  () => import('react-leaflet').then((mod) => mod.GeoJSON),
  { ssr: false }
)
const Popup = dynamic(
  () => import('react-leaflet').then((mod) => mod.Popup),
  { ssr: false }
)
const Tooltip = dynamic(
  () => import('react-leaflet').then((mod) => mod.Tooltip),
  { ssr: false }
)

interface ElectionMapProps {
  winners: ConstituencyWinner[]
  selectedAlliance: string | null
  onConstituencyClick?: (constituency: ConstituencyWinner) => void
}

// Approximate district center coordinates for Tamil Nadu
// TODO: Replace with actual constituency centroids from GeoJSON
const DISTRICT_COORDINATES: Record<string, [number, number]> = {
  'Chennai': [13.0827, 80.2707],
  'CHENNAI': [13.0827, 80.2707],
  'Coimbatore': [11.0168, 76.9558],
  'COIMBATORE': [11.0168, 76.9558],
  'Madurai': [9.9252, 78.1198],
  'MADURAI': [9.9252, 78.1198],
  'Tiruchirappalli': [10.7905, 78.7047],
  'TIRUCHIRAPPALLI': [10.7905, 78.7047],
  'Salem': [11.6643, 78.1460],
  'SALEM': [11.6643, 78.1460],
  'Tirunelveli': [8.7139, 77.7567],
  'TIRUNELVELI': [8.7139, 77.7567],
  'Thanjavur': [10.7870, 79.1378],
  'THANJAVUR': [10.7870, 79.1378],
  'Vellore': [12.9165, 79.1325],
  'VELLORE': [12.9165, 79.1325],
  'Erode': [11.3410, 77.7172],
  'ERODE': [11.3410, 77.7172],
  'Tiruppur': [11.1085, 77.3411],
  'TIRUPPUR': [11.1085, 77.3411],
  'Dindigul': [10.3624, 77.9695],
  'DINDIGUL': [10.3624, 77.9695],
  'Kancheepuram': [12.8342, 79.7036],
  'KANCHEEPURAM': [12.8342, 79.7036],
  'Cuddalore': [11.7480, 79.7714],
  'CUDDALORE': [11.7480, 79.7714],
  'Villupuram': [11.9401, 79.4861],
  'VILLUPURAM': [11.9401, 79.4861],
  'Nagapattinam': [10.7672, 79.8449],
  'NAGAPATTINAM': [10.7672, 79.8449],
  'Ramanathapuram': [9.3639, 78.8395],
  'RAMANATHAPURAM': [9.3639, 78.8395],
  'Sivaganga': [9.8433, 78.4809],
  'SIVAGANGA': [9.8433, 78.4809],
  'Virudhunagar': [9.5680, 77.9624],
  'VIRUDHUNAGAR': [9.5680, 77.9624],
  'Theni': [10.0104, 77.4768],
  'THENI': [10.0104, 77.4768],
  'Thoothukudi': [8.7642, 78.1348],
  'THOOTHUKUDI': [8.7642, 78.1348],
  'Tiruvannamalai': [12.2253, 79.0747],
  'TIRUVANNAMALAI': [12.2253, 79.0747],
  'Krishnagiri': [12.5186, 78.2137],
  'KRISHNAGIRI': [12.5186, 78.2137],
  'Dharmapuri': [12.1357, 78.1602],
  'DHARMAPURI': [12.1357, 78.1602],
  'Namakkal': [11.2342, 78.1674],
  'NAMAKKAL': [11.2342, 78.1674],
  'Karur': [10.9601, 78.0766],
  'KARUR': [10.9601, 78.0766],
  'Pudukkottai': [10.3833, 78.8001],
  'PUDUKKOTTAI': [10.3833, 78.8001],
  'Ariyalur': [11.1401, 79.0757],
  'ARIYALUR': [11.1401, 79.0757],
  'Perambalur': [11.2320, 78.8806],
  'PERAMBALUR': [11.2320, 78.8806],
  'Nilgiris': [11.4916, 76.7337],
  'NILGIRIS': [11.4916, 76.7337],
  'Kanyakumari': [8.0883, 77.5385],
  'KANYAKUMARI': [8.0883, 77.5385],
  'Thiruvallur': [13.1431, 79.9086],
  'THIRUVALLUR': [13.1431, 79.9086],
  'TIRUVALLUR': [13.1431, 79.9086],
  'Chengalpattu': [12.6819, 79.9888],
  'CHENGALPATTU': [12.6819, 79.9888],
  'State_Wide': [11.1271, 78.6569],  // Center of Tamil Nadu
}

// Normalize constituency names for matching
// Handles differences between GeoJSON and database constituency names
function normalizeConstituencyName(name: string): string {
  if (!name) return ''
  
  // Convert to uppercase and trim
  let normalized = name.toUpperCase().trim()
  
  // Common normalization patterns
  const replacements: [RegExp, string][] = [
    [/\(/g, ' '],           // Remove opening parentheses
    [/\)/g, ' '],           // Remove closing parentheses
    [/\./g, ' '],           // Replace dots with spaces
    [/[-_]/g, ' '],         // Replace hyphens/underscores with spaces
    [/\s+/g, ' '],          // Normalize multiple spaces to single space
    [/^DR\s+/, 'DR. '],     // Normalize "DR " to "DR. "
    [/^DR\.\s*/, 'DR. '],   // Normalize "DR." to "DR. "
  ]
  
  for (const [pattern, replacement] of replacements) {
    normalized = normalized.replace(pattern, replacement)
  }
  
  // Specific name mappings for known mismatches
  const nameMappings: Record<string, string> = {
    'AVANASHI': 'AVINASHI',
    'KANNIYAKUMARI': 'KANYAKUMARI',
    'METTUPPALAYAM': 'METTUPALAYAM',
    'MUDHUKULATHUR': 'MUDUKULATHUR',
    'PAPPIREDDIPATTI': 'PAPPIREDDIPPATTI',
    'PARAMATHI-VELUR': 'PARAMATHI VELUR',
    'SHOZHINGANALLUR': 'SHOLINGANALLUR',
    'THOOTHUKKUDI': 'THOOTHUKUDI',
    'TIRUPPATTUR': 'TIRUPPATHUR',
    'GANDARVAKKOTTAI': 'GANDARVAKOTTAI',
    'COLACHAL': 'COLACHEL',
    'DR.RADHAKRISHNAN NAGAR': 'DR. RADHAKRISHNAN NAGAR',
  }
  
  // Apply mappings
  normalized = nameMappings[normalized] || normalized
  
  // Final trim
  return normalized.trim()
}

export default function ElectionMap({ winners, selectedAlliance, onConstituencyClick }: ElectionMapProps) {
  const [mounted, setMounted] = useState(false)
  const [geojsonData, setGeojsonData] = useState<any>(null)

  useEffect(() => {
    setMounted(true)
    // Load GeoJSON data
    fetch('/tn_constituencies.geojson')
      .then(res => res.json())
      .then(data => setGeojsonData(data))
      .catch(err => {
        console.error('Failed to load GeoJSON:', err)
        setGeojsonData(null)
      })
  }, [])

  // Filter winners by selected alliance (must be before early return)
  const filteredWinners = selectedAlliance
    ? winners.filter(w => w.predicted_alliance === selectedAlliance)
    : winners

  // Create a map of constituency name -> winner data for quick lookup
  // MUST be called before any conditional returns (React hooks rule)
  const winnersMap = useMemo(() => {
    const map = new Map<string, ConstituencyWinner>()
    filteredWinners.forEach(winner => {
      // Normalize constituency name for matching (uppercase, trim, handle variations)
      const normalized = normalizeConstituencyName(winner.constituency_name)
      map.set(normalized, winner)
      
      // Also store original name for fallback
      const originalNormalized = winner.constituency_name.toUpperCase().trim()
      if (originalNormalized !== normalized) {
        map.set(originalNormalized, winner)
      }
    })
    return map
  }, [filteredWinners])

  // Early return AFTER all hooks are called
  if (!mounted) {
    return (
      <div className="h-full w-full bg-bg-card rounded-xl flex items-center justify-center">
        <div className="text-gray-400">Loading map...</div>
      </div>
    )
  }

  // Style function for GeoJSON features
  const getStyle = (feature: any) => {
    const constituencyName = feature.properties.Constituency || ''
    const normalizedGeoName = normalizeConstituencyName(constituencyName)
    const winner = winnersMap.get(normalizedGeoName) || winnersMap.get(constituencyName.toUpperCase().trim())
    
    if (!winner) {
      return {
        fillColor: '#64748B', // Unknown/gray
        color: '#475569',
        weight: 1,
        fillOpacity: 0.3,
        opacity: 0.5
      }
    }

    const color = getAllianceColor(winner.predicted_alliance)
    const sentimentStrength = Math.abs(winner.sentiment_score)
    const fillOpacity = 0.5 + sentimentStrength * 0.3

    return {
      fillColor: color,
      color: '#fff',
      weight: winner.is_flip ? 3 : 2,
      fillOpacity: fillOpacity,
      opacity: 0.8
    }
  }

  // Event handlers for GeoJSON
  const onEachFeature = (feature: any, layer: any) => {
    const constituencyName = feature.properties.Constituency || ''
    const normalizedGeoName = normalizeConstituencyName(constituencyName)
    const winner = winnersMap.get(normalizedGeoName) || winnersMap.get(constituencyName.toUpperCase().trim())
    
    if (!winner) {
      layer.bindTooltip(`${feature.properties.Constituency || 'Unknown'}<br/>No prediction`, {
        permanent: false,
        direction: 'center',
        className: 'custom-tooltip'
      })
      return
    }

    const color = getAllianceColor(winner.predicted_alliance)
    
    layer.bindTooltip(
      `<div style="text-align: center;">
        <strong>${winner.constituency_name}</strong><br/>
        <span style="background-color: ${color}; padding: 2px 6px; border-radius: 3px; color: ${winner.predicted_alliance.includes('TVK') ? '#000' : '#fff'}; font-size: 11px;">
          ${winner.predicted_alliance.replace('_', ' ')}
        </span>
      </div>`,
      {
        permanent: false,
        direction: 'center',
        className: 'custom-tooltip'
      }
    )

    layer.bindPopup(
      `<div style="min-width: 200px; color: #fff; background-color: #1F2937; padding: 12px; border-radius: 8px;">
        <h3 style="font-weight: bold; margin-bottom: 8px; color: #fff; font-size: 16px;">${winner.constituency_name}</h3>
        <p style="color: #9CA3AF; font-size: 12px; margin-bottom: 12px;">${winner.district} District</p>
        
        <div style="display: flex; justify-content: space-between; margin-bottom: 6px;">
          <span style="color: #9CA3AF;">Predicted Winner</span>
          <span style="background-color: ${color}; padding: 2px 8px; border-radius: 4px; color: ${winner.predicted_alliance.includes('TVK') ? '#000' : '#fff'}; font-size: 11px; font-weight: 500;">
            ${winner.predicted_alliance.replace('_', ' ')}
          </span>
        </div>
        
        ${winner.incumbent_2021 ? `
        <div style="display: flex; justify-content: space-between; margin-bottom: 6px;">
          <span style="color: #9CA3AF;">2021 Winner</span>
          <span style="color: #fff;">${winner.incumbent_2021.replace('_', ' ')}</span>
        </div>
        ` : ''}
        
        ${winner.is_flip ? `
        <div style="background-color: #FEF3C7; color: #92400E; padding: 4px 8px; border-radius: 4px; margin: 8px 0; font-size: 11px; text-align: center;">
          ⚠️ Potential Flip
        </div>
        ` : ''}
        
        <div style="display: flex; justify-content: space-between; margin-bottom: 6px;">
          <span style="color: #9CA3AF;">Sentiment</span>
          <span style="color: ${winner.sentiment_score > 0 ? '#10B981' : winner.sentiment_score < 0 ? '#EF4444' : '#9CA3AF'};">
            ${winner.sentiment_score > 0 ? '+' : ''}${(winner.sentiment_score * 100).toFixed(1)}%
          </span>
        </div>
        
        <div style="display: flex; justify-content: space-between; margin-bottom: 6px;">
          <span style="color: #9CA3AF;">Confidence</span>
          <span style="color: #fff;">${(winner.confidence_weight * 100).toFixed(0)}%</span>
        </div>
        
        <div style="display: flex; justify-content: space-between;">
          <span style="color: #9CA3AF;">Sources</span>
          <span style="color: #fff;">${winner.source_count}</span>
        </div>
      </div>`,
      {
        className: 'custom-popup',
        maxWidth: 300
      }
    )

    layer.on({
      click: () => {
        onConstituencyClick?.(winner)
      }
    })
  }

  return (
    <MapContainer
      center={[11.1271, 78.6569]}  // Center of Tamil Nadu
      zoom={7}
      className="h-full w-full rounded-xl"
      style={{ background: '#1E293B' }}
    >
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
        url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
      />
      
      {geojsonData && (
        <GeoJSON
          data={geojsonData}
          style={getStyle}
          onEachFeature={onEachFeature}
        />
      )}
    </MapContainer>
  )
}

// Legend component
export function MapLegend() {
  return (
    <div className="bg-bg-card/90 backdrop-blur-sm rounded-lg p-4 shadow-lg">
      <h4 className="text-sm font-semibold text-white mb-3">Alliance Colors</h4>
      <div className="space-y-2">
        {Object.entries(ALLIANCE_COLORS).slice(0, 6).map(([alliance, color]) => (
          <div key={alliance} className="flex items-center gap-2">
            <div 
              className="w-4 h-4 rounded-full"
              style={{ backgroundColor: color }}
            />
            <span className="text-xs text-gray-300">{alliance.replace('_', ' ')}</span>
          </div>
        ))}
      </div>
    </div>
  )
}
